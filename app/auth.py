from typing import Dict, Optional
import aiohttp
import asyncio
from datetime import datetime, timedelta
from .config import ConfigManager, AuthScheme, CustomerDomain
from .logging_config import get_logger

logger = get_logger("auth_handler")

class NSAuthHandler:
    _instance = None
    
    def __new__(cls, config_manager: ConfigManager = None):
        if cls._instance is None:
            if config_manager is None:
                raise ValueError("Config manager required for first NSAuthHandler instantiation")
            cls._instance = super(NSAuthHandler, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config_manager: ConfigManager = None):
        if self._initialized:
            return
            
        self.config = config_manager
        self.token_cache: Dict[str, Dict] = {}  # domain_id -> {token, expiry}
        self._lock = asyncio.Lock()
        logger.info(f"Initialized NSAuthHandler singleton for {len(config_manager.domain_map)} domains")
        self._initialized = True

    async def get_token(self, domain_id: str) -> Optional[str]:
        """Get a valid token for the specified domain"""
        async with self._lock:
            logger.debug(f"Token requested for domain: {domain_id}")
            
            cached = self.token_cache.get(domain_id)
            if cached and cached['expiry'] > datetime.now():
                logger.debug(f"Using cached token for {domain_id}, expires in {cached['expiry'] - datetime.now()}")
                return cached['token']

            domain_config = self.config.get_domain_config(domain_id)
            if not domain_config:
                logger.error(f"No configuration found for domain: {domain_id}")
                return None

            # Find the reseller config for this domain
            reseller = next(
                (r for r in self.config.resellers.values() 
                 if domain_config in r.customer_domains),
                None
            )
            if not reseller:
                logger.error(f"No reseller found for domain: {domain_config.domain_id}")
                return None

            logger.info(
                f"Authenticating domain {domain_id} "
                f"(reseller: {reseller.reseller_id}, "
                f"scheme: {reseller.authentication.scheme})"
            )

            token = await self._authenticate(domain_config)
            if token:
                expiry = datetime.now() + timedelta(hours=1)
                self.token_cache[domain_id] = {
                    'token': token,
                    'expiry': expiry
                }
                logger.info(f"Successfully authenticated domain {domain_id}, token expires at {expiry}")
            else:
                logger.error(f"Authentication failed for domain {domain_id}")
            return token

    async def _authenticate(self, domain_config: CustomerDomain) -> Optional[str]:
        """Authenticate with NS API based on the domain's configuration"""
        # Find the reseller config for this domain
        reseller = next(
            (r for r in self.config.resellers.values() 
             if domain_config in r.customer_domains),
            None
        )
        if not reseller:
            logger.error(f"No reseller found for domain: {domain_config.domain_id}")
            return None

        # Extract territory ID from domain_id (format: domain.territory.service)
        territory_id = domain_config.domain_id.split('.')[1]
        # Construct correct NS API auth URL
        auth_url = f"https://{territory_id}-hpbx.dashmanager.com/ns-api/oauth2/token"

        auth = reseller.authentication
        auth_data = {
            "grant_type": "password",
            "client_id": auth.client_id,
            "client_secret": auth.client_secret,
        }

        if auth.scheme == AuthScheme.RESELLER_LEVEL:
            auth_data.update({
                "username": auth.username,
                "password": auth.password
            })
            logger.debug(f"Using reseller-level auth for {domain_config.domain_id}")
        else:  # DOMAIN_OFFICE_MANAGER
            if not domain_config.authentication:
                logger.error(f"No domain authentication found for: {domain_config.domain_id}")
                return None
            auth_data.update({
                "username": domain_config.authentication.username,
                "password": domain_config.authentication.password
            })
            logger.debug(f"Using domain-level auth for {domain_config.domain_id}")

        try:
            async with aiohttp.ClientSession() as session:
                logger.info(
                    f"Authenticating domain {domain_config.domain_id} "
                    f"with {auth_url} (client_id: {auth.client_id})"
                )
                async with session.post(
                    auth_url,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Accept": "application/json"
                    },
                    data=auth_data
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"Authentication successful for domain {domain_config.domain_id}")
                        return data.get("access_token")
                    else:
                        error_text = await response.text()
                        logger.error(
                            f"Authentication failed for domain {domain_config.domain_id}: "
                            f"Status {response.status}, Error: {error_text}, "
                            f"URL: {auth_url}"
                        )
                        return None
        except Exception as e:
            logger.error(
                f"Authentication error for domain {domain_config.domain_id}: "
                f"{str(e)}, URL: {auth_url}"
            )
            return None

    async def clear_token(self, domain_id: str):
        """Clear the cached token for a domain"""
        async with self._lock:
            self.token_cache.pop(domain_id, None) 
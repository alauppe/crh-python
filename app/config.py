from pathlib import Path
from typing import Dict, Optional, List
import json
from dataclasses import dataclass, field
from enum import Enum

class AuthScheme(Enum):
    RESELLER_LEVEL = "reseller-level"
    DOMAIN_OFFICE_MANAGER = "domain-office-manager"

@dataclass
class Authentication:
    scheme: AuthScheme
    client_id: str
    client_secret: str
    username: Optional[str] = None
    password: Optional[str] = None

@dataclass
class Storage:
    type: str
    bucket_name: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    container_name: Optional[str] = None
    account_name: Optional[str] = None
    account_key: Optional[str] = None
    server: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

DEFAULT_WORKER_CONCURRENCY = {
    "cdr_fetcher": 1,
    "recording_query": 2,
    "recording_handler": 2,
    "transcription": 1
}

@dataclass
class CustomerDomain:
    domain_id: str
    authentication: Optional[Authentication] = None
    storage: Optional[Storage] = None
    openai_api_key: Optional[str] = None
    cdr_fetcher: Dict = None
    recording_query: Dict = None
    recording_handler: Dict = None
    call_recording_handler: Dict = None
    call_transcription: Dict = None
    call_summary: Dict = None
    worker_concurrency: Dict = field(
        default_factory=lambda: DEFAULT_WORKER_CONCURRENCY.copy()
    )

    def __init__(self, domain_id: str, **kwargs):
        self.domain_id = domain_id
        self.authentication = kwargs.get('authentication')
        self.storage = kwargs.get('storage')
        self.openai_api_key = kwargs.get('openai_api_key')
        self.cdr_fetcher = kwargs.get('cdr_fetcher')
        self.recording_query = kwargs.get('recording_query')
        self.recording_handler = kwargs.get('recording_handler')
        self.call_recording_handler = kwargs.get('call_recording_handler')
        self.call_transcription = kwargs.get('call_transcription')
        self.call_summary = kwargs.get('call_summary')
        self.worker_concurrency = {
            **DEFAULT_WORKER_CONCURRENCY,
            **(kwargs.get('worker_concurrency') or {})
        }

@dataclass
class Reseller:
    reseller_id: str
    authentication: Authentication
    customer_domains: List[CustomerDomain]

class ConfigManager:
    def __init__(self, config_path: str = "config.json"):
        self.config_path = Path(config_path)
        self.resellers: Dict[str, Reseller] = {}
        self.domain_map: Dict[str, CustomerDomain] = {}
        self.openai_api_key: Optional[str] = None
        self.load_config()

    def load_config(self):
        """Load and validate configuration from JSON file"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with open(self.config_path) as f:
            config = json.load(f)

        self.openai_api_key = config.get("openai_api_key")

        for reseller_data in config.get("resellers", []):
            # Create Authentication object
            auth_data = reseller_data["authentication"]
            auth = Authentication(
                scheme=AuthScheme(auth_data["scheme"]),
                client_id=auth_data["client_id"],
                client_secret=auth_data["client_secret"],
                username=auth_data.get("username"),
                password=auth_data.get("password")
            )

            # Create CustomerDomain objects
            domains = []
            for domain_data in reseller_data.get("customer_domains", []):
                # Create Storage object if present
                storage = None
                if "storage" in domain_data:
                    storage = Storage(**domain_data["storage"])

                # Create domain-level Authentication object if present
                domain_auth = None
                if "authentication" in domain_data:
                    domain_auth = Authentication(
                        scheme=auth.scheme,
                        client_id=auth.client_id,
                        client_secret=auth.client_secret,
                        **domain_data["authentication"]
                    )

                domain = CustomerDomain(
                    domain_id=domain_data["domain_id"],
                    authentication=domain_auth,
                    storage=storage,
                    openai_api_key=domain_data.get("openai_api_key"),
                    cdr_fetcher=domain_data.get("cdr_fetcher"),
                    recording_query=domain_data.get("recording_query"),
                    recording_handler=domain_data.get("recording_handler"),
                    call_recording_handler=domain_data.get("call_recording_handler"),
                    call_transcription=domain_data.get("call_transcription"),
                    call_summary=domain_data.get("call_summary"),
                    worker_concurrency=domain_data.get("worker_concurrency")
                )
                domains.append(domain)
                self.domain_map[domain.domain_id] = domain

            # Create Reseller object
            reseller = Reseller(
                reseller_id=reseller_data["reseller_id"],
                authentication=auth,
                customer_domains=domains
            )
            self.resellers[reseller.reseller_id] = reseller

    def get_domain_config(self, domain_id: str) -> Optional[CustomerDomain]:
        """Get configuration for a specific domain"""
        return self.domain_map.get(domain_id)

    def get_reseller_config(self, reseller_id: str) -> Optional[Reseller]:
        """Get configuration for a specific reseller"""
        return self.resellers.get(reseller_id) 
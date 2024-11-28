# Call Recording System

This system handles CDR (Call Detail Record) fetching and recording queries, providing a robust solution for managing and retrieving call recordings across multiple storage backends.

## System Overview

The application consists of several worker components that handle different aspects of call recording management:

1. **CDR Fetcher Worker**: Retrieves Call Detail Records from various sources and processes them for storage
2. **Recording Query Worker**: Handles queries for call recordings and manages retrieval from different storage backends
3. **Recording Handler Worker**: Manages the storage of recordings across multiple backend systems
4. **Transcription Worker**: Processes audio recordings to generate text transcriptions
5. **Summary Worker**: Generates concise summaries of call transcriptions using LLM technology

## Features

- Multi-backend storage support (AWS S3, Backblaze B2, FTP, FTPS, SFTP)
- Automated call transcription
- AI-powered call summarization
- Configurable retry mechanisms for all operations
- Concurrent processing capabilities
- Flexible storage cleanup options

## Project Structure

    app/
    ├── config/               # Configuration files and handlers
    ├── workers/             # Worker components
    │   ├── cdr_fetcher/     # CDR retrieval and processing
    │   ├── recording_query/ # Recording retrieval management
    │   ├── recording_handler/ # Storage backend management
    │   ├── transcription/   # Audio transcription processing
    │   └── summa/          # Summary generation
    ├── data/               # Local data storage
    ├── logs/               # Application logs
    └── state/              # Worker state management

## Setup

### Windows
Run setup.bat

### Linux/MacOS
Run ./setup.sh

## Configuration

The system is configured through a config.json file. Key configuration areas include:

- Storage backend credentials and settings
- Worker-specific configurations:
  - Retry intervals
  - Polling intervals
  - Cleanup settings
- API endpoints and authentication

See config.json.example for a complete configuration template.

## Worker Components

### CDR Fetcher
- Retrieves call detail records from various sources
- Processes and stores records in the database
- Configurable polling and retry intervals

### Recording Query
- Handles requests for call recordings
- Manages retrieval from multiple storage backends
- Implements concurrent processing

### Transcription Worker
- Processes audio recordings to text
- Configurable API integration
- Supports transcript cleanup after processing

### Summary Worker
- Generates AI-powered call summaries
- Configurable summary retention
- Supports multiple output formats

## Storage Backends

Supported storage systems:
- AWS S3
- Backblaze B2
- FTP
- FTPS
- SFTP

Each backend supports:
- Individual enable/disable toggles
- Custom retry intervals
- Path configurations
- Authentication settings

## Development

To set up the development environment:

1. Clone the repository
2. Run the appropriate setup script:
   - Windows: setup.bat
   - Linux/MacOS: ./setup.sh
3. Copy config.json.example to config.json and configure as needed
4. Install dependencies: pip install -r requirements.txt

## Requirements

See requirements.txt for a complete list of dependencies.
# Call Recording System

This system handles CDR (Call Detail Record) fetching and recording queries, providing a robust solution for managing and retrieving call recordings across multiple storage backends.

## System Overview

The application consists of two main worker components that handle different aspects of call recording management:

1. **CDR Fetcher Worker**: Retrieves Call Detail Records from various sources and processes them for storage
2. **Recording Query Worker**: Handles queries for call recordings and manages retrieval from different storage backends

## Project Structure

app/
├── config
# yaf-ai-pipeline

## Overview

This project monitors and ingests aviation accessibility rules, including airline wheelchair policies, battery restrictions, and regulatory guidelines.  
It detects updates across websites, PDFs, and other document sources, then processes them into **structured rules** for downstream applications such as chatbots, analytics, or APIs.

---

## Features

- Web scraping of public sources (HTML pages and PDFs)  
- Document chunking and processing for AI pipelines  
- Rule extraction using AI (LLM embeddings and summarization)  
- Change detection and versioning of rules  
- Storage in **Firestore** for easy API access  

---

## Tech Stack

- **Python 3.11+** – main ingestion and AI processing pipeline  
- **BeautifulSoup** – web scraping  
- **pdfplumber** – PDF text extraction  
- **google-genai** – embeddings, summarization, and rule extraction  
- **google-cloud-firestore** – Firestore database integration  
- **python-dotenv** – environment variable management  
- **pytest** – unit testing and validation  
- **Selenium (with undetected-chromedriver)** – Extracts data from dynamic, JavaScript-heavy sites while bypassing anti-bot protections.

---

## Architecture Overview
1. **Ingestion Jobs**  
   Scheduled jobs that scrape airline, airport, and regulatory sources automatically.  

2. **Document Storage**  
   Raw HTML, PDF, or docx documents are stored in Firestore or cloud storage for reference and processing.  

3. **Chunking**  
   Large documents are split into manageable text chunks suitable for AI processing.  

4. **Rule Extraction**  
   Analyzes chunks and generates structured rules, including title, condition, action, and confidence.  

5. **Rules Storage**  
   Validated rules are stored in Firestore for downstream consumption via API or other applications.  


## Getting Started
- Clone the repository and set up a Python environment
	- requirements.txt contains all necessary dependencies
	- create a .env file for API keys and configuration
- pipeline_flow_doc_process.py contains the main ingestion and processing logic
- main.py serves as the entry point for running the pipeline

## Module Breakdown
- **Chunker:** Handles splitting large documents into smaller, manageable pieces for AI processing.
- **ChunkStore:** Manages storage and retrieval of document chunks
- **Extractor:** Uses LLMs to analyze text chunks and extract structured rules based on predefined templates.
- **Firestore:** Stores raw documents, processed chunks, and extracted rules for easy retrieval and versioning. 
- **Models:** Defines data models for documents, chunks, and rules to ensure consistent structure and validation across the pipeline.
- **Parser:** Parses different document formats (HTML, PDF, JSON, TXT) and extracting relevant text for processing.
	- Contains logic for scraping various sources, including airline websites and regulatory bodies, to gather raw data for parsing.
- **Sources:** Currently includes sources until firestore implemented
	- May be changed to include configurations and definitions for different data sources, such as URLs, scraping rules, and update frequencies. (may be placed in Firestore)
- **Utils:** Utility functions for common tasks such as logging, error handling, and configuration management.
- **Validator:** Validates extracted rules against predefined criteria to ensure accuracy and relevance before storage
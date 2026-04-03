# yaf-ai-pipeline

## Overview

This project monitors and ingests aviation accessibility rules, including airline wheelchair policies, battery restrictions, and regulatory guidelines.  
It detects updates across websites, PDFs, and other document sources, then processes them into **structured rules** for downstream applications such as chatbots, analytics, or APIs.

---

## Features

- Web scraping of public sources (HTML pages and PDFs)  
- Document chunking and processing for AI pipelines  
- Rule extraction using AI (xAI embeddings and summarization)  
- Change detection and versioning of rules  
- Storage in **Firestore** for easy API access  

---

## Tech Stack

- **Python 3.11+** – main ingestion and AI processing pipeline  
- **BeautifulSoup** – web scraping  
- **pdfplumber** – PDF text extraction  
- **xAI SDK** – embeddings, summarization, and rule extraction  
- **google-cloud-firestore** – Firestore database integration  
- **python-dotenv** – environment variable management  
- **pytest** – unit testing and validation  

---

## Architecture Overview
1. **Ingestion Jobs**  
   Scheduled jobs that scrape airline, airport, and regulatory sources automatically.  

2. **Document Storage**  
   Raw HTML, PDF, or docx documents are stored in Firestore or cloud storage for reference and processing.  

3. **Chunking**  
   Large documents are split into manageable text chunks suitable for AI processing.  

4. **Rule Extraction**  
   The xAI model analyzes chunks and generates structured rules, including title, condition, action, and confidence.  

5. **Rules Storage**  
   Validated rules are stored in Firestore for downstream consumption via API or other applications.  
# BDO Market Scraper

A robust, automated pipeline to scrape and store Black Desert Online market data, optimized for long-term trend analysis.

## 📊 Live Status
| Workflow | Status |
| :--- | :--- |
| **Pearl Items** | [![Scrape BDO Pearl Items](https://github.com/Fayiette/bdo-cm/actions/workflows/pearl-items.yml/badge.svg?branch=main)](https://github.com/Fayiette/bdo-cm/actions/workflows/pearl-items.yml) | 
| **All Market Items** | [![Scrape BDO All Market Items](https://github.com/Fayiette/bdo-cm/actions/workflows/all-items.yml/badge.svg)](https://github.com/Fayiette/bdo-cm/actions/workflows/all-items.yml) |

---

## 🏗️ Architecture
This project utilizes a stateless architecture to ensure data consistency and minimal overhead.

* **Scraping Engine:** Python-based scripts triggered by GitHub Actions.
* **Storage:** Cloudflare R2 (S3-compatible storage) for low-latency retrieval.

---

## 🛠️ Tech Stack
* **Language:** Python
* **Infrastructure:** GitHub Actions (CI/CD + Runner), Cloudflare R2 (Object Storage), Cloudflare Workers
* **Strategy:** Daily snapshots with overwrite-logic to ensure single-row-per-ID integrity.

# Data Engineering Zoomcamp 2026

[![Course](https://img.shields.io/badge/Course-Data%20Engineering%20Zoomcamp-blue)](https://github.com/DataTalksClub/data-engineering-zoomcamp)
[![Cohort](https://img.shields.io/badge/Cohort-2026-green)](https://github.com/DataTalksClub/data-engineering-zoomcamp)

This repository contains my solutions and projects for the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/) - Cohort 2026.

## 📚 Course Overview

The Data Engineering Zoomcamp is a free online course covering data engineering fundamentals:

- Containerization and Infrastructure as Code
- Workflow Orchestration
- Data Warehousing
- Analytics Engineering
- Batch Processing
- Stream Processing

**Course Repository:** https://github.com/DataTalksClub/data-engineering-zoomcamp

---

## 🗂️ Repository Structure

### [Module 1: Docker & Terraform](./01-docker-terraform/)
**Status:** ✅ Completed | **Date:** January 2026

Topics covered:
- Docker containers and Docker Compose
- PostgreSQL and pgAdmin
- SQL analytics queries
- Terraform basics and GCP provisioning
- Data ingestion pipelines

**View:** [Homework Solutions](./01-docker-terraform/README.md)

---

### [Module 2: Workflow Orchestration](./02-workflow-orchestration/)
**Status:** ⏳ Coming Soon

Topics:
- Workflow orchestration with Mage
- Data pipeline design
- Scheduling and monitoring
- Parameterization and backfills

---

### [Module 3: Data Warehouse](./03-data-warehouse/)
**Status:** ⏳ Coming Soon

Topics:
- BigQuery fundamentals
- Data warehouse design patterns
- Partitioning and clustering
- Optimization best practices

---

### [Module 4: Analytics Engineering](./04-analytics-engineering/)
**Status:** ⏳ Coming Soon

Topics:
- dbt (data build tool)
- Data modeling and transformation
- Testing and documentation
- CI/CD for analytics

---

### [Module 5: Batch Processing](./05-batch/)
**Status:** ⏳ Coming Soon

Topics:
- Apache Spark
- Distributed processing
- Batch processing patterns
- Performance tuning

---

### [Module 6: Streaming](./06-streaming/)
**Status:** ⏳ Coming Soon

Topics:
- Apache Kafka
- Stream processing with Spark Streaming
- Real-time analytics
- Event-driven architectures

---

## 🛠️ Tech Stack

### Infrastructure & Orchestration
- Docker & Docker Compose
- Terraform
- Mage / Airflow

### Data Storage & Processing
- PostgreSQL
- Google BigQuery
- Apache Spark
- Apache Kafka

### Analytics & Transformation
- dbt (data build tool)
- SQL
- Python (pandas, PySpark)

### Cloud Platform
- Google Cloud Platform (GCP)
  - Cloud Storage
  - BigQuery
  - Compute Engine

---

## 🎯 Learning Goals

- [x] Master containerization and infrastructure as code
- [ ] Build production-ready data pipelines
- [ ] Design and optimize data warehouses
- [ ] Implement analytics engineering best practices
- [ ] Handle batch and stream processing at scale
- [ ] Deploy end-to-end data solutions

---

## 📊 Progress Tracker

| Module | Topics | Status | Completion |
|--------|--------|--------|------------|
| **1** | Docker & Terraform | ✅ Complete | Jan 2026 |
| **2** | Workflow Orchestration | ⏳ Not Started | - |
| **3** | Data Warehouse | ⏳ Not Started | - |
| **4** | Analytics Engineering | ⏳ Not Started | - |
| **5** | Batch Processing | ⏳ Not Started | - |
| **6** | Streaming | ⏳ Not Started | - |

---

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Git
- Terraform (for Module 1)
- GCP account (for cloud modules)

### Getting Started

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/data-engineering-zoomcamp.git
cd data-engineering-zoomcamp

# Navigate to a specific module
cd 01-docker-terraform

# Follow the module's README and SETUP guide
cat README.md
```

Each module contains:
- `README.md` - Overview and homework solutions
- `SETUP.md` - Detailed setup instructions
- Configuration files and code
- Documentation

---

## 📝 Project Structure

```
data-engineering-zoomcamp/
├── README.md                           # This file
│
├── 01-docker-terraform/                # Module 1
│   ├── README.md                       # Module overview & solutions
│   ├── SETUP.md                        # Setup instructions
│   ├── docker-compose.yaml
│   ├── queries/                        # SQL queries
│   └── terraform/                      # IaC configuration
│
├── 02-workflow-orchestration/          # Module 2
│   └── README.md
│
├── 03-data-warehouse/                  # Module 3
│   └── README.md
│
├── 04-analytics-engineering/           # Module 4
│   └── README.md
│
├── 05-batch/                           # Module 5
│   └── README.md
│
└── 06-streaming/                       # Module 6
    └── README.md
```

---

## 🎓 Key Learnings

### Module 1: Docker & Terraform
- Containerization fundamentals
- Multi-container orchestration with Docker Compose
- Docker networking and service discovery
- SQL analytics for real-world datasets
- Infrastructure as Code with Terraform
- GCP resource provisioning

*More learnings will be added as I progress through the course.*

---

## 🤝 Acknowledgments

Huge thanks to:
- [DataTalksClub](https://datatalks.club/) for organizing this excellent free course
- **Alexey Grigorev** - Course creator and instructor
- **Ankush Khanna** - Instructor
- **Victoria Perez Mola** - Instructor
- The entire DataTalksClub community for support and collaboration

---

## 📚 Resources

### Course Links
- [Official Repository](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- [Course Website](https://datatalks.club/blog/data-engineering-zoomcamp.html)
- [YouTube Playlist](https://www.youtube.com/playlist?list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
- [Slack Community](https://datatalks.club/slack.html)

### Documentation
- [Docker Docs](https://docs.docker.com/)
- [Terraform Docs](https://www.terraform.io/docs)
- [dbt Docs](https://docs.getdbt.com/)
- [Spark Docs](https://spark.apache.org/docs/latest/)
- [Kafka Docs](https://kafka.apache.org/documentation/)

### Tools
- [Google Cloud Platform](https://cloud.google.com/)
- [pgAdmin](https://www.pgadmin.org/)
- [Mage](https://www.mage.ai/)

---

## 👤 Author

**[Your Name]**
- GitHub: [@your-username](https://github.com/your-username)
- LinkedIn: [your-profile](https://linkedin.com/in/your-profile)
- Email: your.email@example.com

---

## 📄 License

This project contains my personal solutions to the Data Engineering Zoomcamp homework and projects. All course materials are property of DataTalksClub.

---

## 🌟 Show Your Support

If you found this repository helpful:
- ⭐ Star this repository
- 🔄 Fork it for your own learning
- 📢 Share it with others learning data engineering
- 🤝 Connect with me on LinkedIn

---

## 💬 Let's Connect!

Are you also taking this course? Let's connect and learn together!

- Join the [DataTalksClub Slack](https://datatalks.club/slack.html)
- Follow the course on [GitHub](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- Share your progress using `#DataEngineeringZoomcamp`

---

## 📈 Updates

**Latest:** Module 1 (Docker & Terraform) completed - January 2026

*This README will be updated as I progress through the course.*

---

**Made with ❤️ during the Data Engineering Zoomcamp 2026**

*"Data engineering is not just about moving data around; it's about building reliable, scalable systems that empower data-driven decisions."*

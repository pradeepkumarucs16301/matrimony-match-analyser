# Matrimony Match Analyser

> A Databricks-based matrimony matchmaking application that combines a modern data pipeline with traditional Tamil star compatibility rules.

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Gradio](https://img.shields.io/badge/Gradio-FF7C00?style=flat-square&logo=gradio&logoColor=white)](https://www.gradio.app/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat-square)](https://delta.io/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

## Overview

**Matrimony Match Analyser** is a full-stack data engineering and application project built on **Databricks**.

The project collects publicly accessible matrimony profile data, processes and enriches it through a Bronze-to-Gold data pipeline, applies a traditional Tamil star compatibility reference, and exposes the curated data through a Gradio-based Databricks App.

The project demonstrates an end-to-end workflow covering:

- Web data extraction
- PySpark and Delta Lake processing
- Data enrichment and transformation
- Compatibility-rule modelling
- Unity Catalog storage
- Databricks Apps
- OAuth-based application access
- User registration and OTP verification
- Profile filtering and discovery
- Interactive analytics and visualizations

> **Important:** Compatibility results are based on the traditional reference rules implemented in this project. They are provided as a cultural/traditional matching aid and should not be treated as a scientific, medical, psychological, or relationship assessment.

---

## Key Capabilities

### Data Engineering

- Scrapes profile listings and detailed profile pages.
- Handles ASP.NET postback pagination.
- Identifies both male and female profiles.
- Normalizes scraped attributes into structured columns.
- Stores raw data in a Delta Bronze table.
- Transforms and enriches data into a curated Gold table.
- Builds a reusable star compatibility lookup table.
- Uses Unity Catalog for tables and profile-image storage.

The main scraper notebook also supports **incremental** and **full refresh** execution modes. Incremental mode is intended for faster recurring updates, while full mode rebuilds the dataset.

### Matchmaking & Discovery

The application provides filtering and matching capabilities based on fields such as:

- Gender
- Birth year
- Community / sub-caste
- Birth star (Nakshatra)
- Rasi
- Profile registration number
- Profile exclusion lists
- Traditional star compatibility categories

### Compatibility Engine

The transformation layer parses:

- Date of birth
- Birth year
- Birth place
- Star
- Rasi
- Contact information

It also creates the compatibility dataset used by the application.

Compatibility categories currently include:

- **Utthamam**
- **Madhyamam**

The implemented rules are a traditional Tamil Thirumana Porutham reference and should be interpreted accordingly.

### Authentication

The application includes:

- User registration
- Password hashing using PBKDF2-HMAC-SHA256
- Email OTP verification
- OTP expiry and one-time-use validation
- Login authentication
- Session state
- Authentication audit logging

### Analytics

The application provides interactive visualizations for areas such as:

- Star distribution
- Rasi distribution
- Birth-year distribution
- Gender distribution

Charts are generated using Plotly.

### Profile Images

Profile images are stored in a Unity Catalog Volume and retrieved by the Databricks App through the Databricks SDK.

---

## Architecture

### End-to-End Data Flow

```text
                    Public Profile Source
                            │
                            ▼
                 ┌─────────────────────┐
                 │   Scraper Layer     │
                 │   Python + Requests │
                 │   BeautifulSoup     │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │    Bronze Layer     │
                 │  Raw Profile Data   │
                 │  Delta Lake Table   │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │   Transform Layer   │
                 │      PySpark        │
                 │  Parsing + Enrichment│
                 └──────────┬──────────┘
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
   ┌─────────────────────┐     ┌────────────────────────┐
   │     Gold Layer      │     │ Compatibility Reference│
   │ Curated Profiles    │     │    Star ↔ Star Rules   │
   └──────────┬──────────┘     └────────────┬───────────┘
              │                             │
              └─────────────┬───────────────┘
                            ▼
                 ┌─────────────────────┐
                 │    Databricks App   │
                 │  Gradio + Python    │
                 └──────────┬──────────┘
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
          Filtering     Matching      Analytics
              │             │             │
              └─────────────┴─────────────┘
                            │
                            ▼
                         End User
```

### Databricks Architecture

```text
┌──────────────────────────────────────────────────────┐
│                  Databricks Workspace                │
│                                                      │
│  ┌────────────────────────────────────────────────┐  │
│  │              Data Engineering                  │  │
│  │                                                │  │
│  │  Scraper → Bronze → Transform → Gold           │  │
│  │                                                │  │
│  │  PySpark / Delta Lake / Unity Catalog          │  │
│  └───────────────────────┬────────────────────────┘  │
│                          │                           │
│                          ▼                           │
│  ┌────────────────────────────────────────────────┐  │
│  │               Databricks App                   │  │
│  │                                                │  │
│  │       Gradio UI + Python Application           │  │
│  │                                                │  │
│  │  Authentication │ Filtering │ Matching │ Charts│  │
│  └───────────────────────┬────────────────────────┘  │
│                          │                           │
│                          ▼                           │
│  ┌────────────────────────────────────────────────┐  │
│  │               Unity Catalog                   │  │
│  │                                                │  │
│  │  gold_profiles                                │  │
│  │  star_compatibility                            │  │
│  │  app_users                                     │  │
│  │  app_otp_codes                                 │  │
│  │  app_audit_logs                                │  │
│  │  profile_images Volume                         │  │
│  └────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Data Platform | Databricks | Data engineering and application platform |
| Processing | PySpark | Transformation and enrichment |
| Storage | Delta Lake | Transactional data storage |
| Governance | Unity Catalog | Tables, volumes, permissions and governance |
| Web Scraping | Requests + BeautifulSoup | Profile extraction |
| Application | Python + Gradio | User-facing web application |
| Analytics | Pandas + Plotly | Filtering and interactive charts |
| Authentication | PBKDF2-HMAC-SHA256 + OTP | User authentication |
| Email | SMTP | OTP delivery |
| Databricks Access | Databricks SDK + OAuth | Application-to-platform access |
| Hosting | Databricks Apps | Application deployment |

---

## Repository Structure

```text
matrimony-match-analyser/
│
├── app.py
│   └── Main Gradio application
│
├── auth.py
│   └── Registration, OTP verification, login and audit logging
│
├── app.yaml
│   └── Databricks App configuration
│
├── 01_scrapper_notebook.py
│   └── Bronze ingestion / scraping notebook
│
├── 02_transform_notebook.py
│   └── Gold transformation and compatibility engine notebook
│
├── scrapper.py
│   └── Standalone scraper implementation
│
├── transform.py
│   └── Standalone transformation implementation
│
├── requirements.txt
│   └── Python dependencies
│
├── README.md
│   └── Project documentation
│
└── LICENSE
    └── MIT License
```

### Core Components

#### `01_scrapper_notebook.py`

Responsible for the Bronze ingestion layer.

Key responsibilities include:

- Loading the profile search page
- Handling ASP.NET ViewState/postback navigation
- Traversing profile pages
- Extracting profile attributes
- Identifying male and female registrations
- Deduplicating profiles
- Writing raw data to:

```text
matrimony.default.bronze_profiles
```

The current notebook includes execution modes for:

```text
incremental
full
```

#### `02_transform_notebook.py`

Responsible for the Gold layer and compatibility processing.

Key responsibilities include:

- Reading Bronze data
- Parsing profile attributes
- Extracting birth year, birth place, star and rasi
- Normalizing contact information
- Enriching profile data
- Building the star compatibility lookup
- Writing curated data to:

```text
matrimony.default.gold_profiles
matrimony.default.star_compatibility
```

#### `app.py`

The main Databricks App.

Responsibilities include:

- Gradio UI
- Databricks SQL connectivity
- Gold data loading
- Profile filtering
- Compatibility matching
- Profile viewing
- Image retrieval
- Interactive charts
- Session handling
- Application logging

#### `auth.py`

Authentication module providing:

- Signup
- Password hashing
- OTP generation
- OTP email delivery
- OTP verification
- Login
- Audit logging

---

## Data Model

The application uses the following primary Unity Catalog objects.

### Bronze

```text
matrimony.default.bronze_profiles
```

Contains raw profile data collected during the scraping stage.

### Gold

```text
matrimony.default.gold_profiles
```

Contains cleaned and enriched profile data used by the application.

Representative attributes include:

```text
reg_no
name
dob
birth_year
birth_place
star
rasi
caste
sub_caste
marital_status
diet
qualification
job
contact_person
contact_no
email_id
gender
image_volume_path
best_match_stars
good_match_stars
```

### Compatibility

```text
matrimony.default.star_compatibility
```

Stores the star-to-star compatibility reference used by the matching engine.

Representative structure:

```text
boy_star
girl_star
match_level
```

### Authentication

```text
matrimony.default.app_users
matrimony.default.app_otp_codes
matrimony.default.app_audit_logs
```

These tables support account management, OTP verification and authentication auditing.

### Image Storage

```text
matrimony.default.profile_images
```

A Unity Catalog Volume used for profile images.

---

## Prerequisites

Before deploying the project, you should have:

1. A Databricks workspace
2. Unity Catalog enabled
3. A Databricks SQL Warehouse
4. Permission to create/use the required catalog, schema, tables and volume
5. A Gmail account with an App Password if OTP email delivery is required
6. Python 3.9+ for local dependency management, if required

---

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/<YOUR_USERNAME>/matrimony-match-analyser.git
cd matrimony-match-analyser
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

> The application is intended to run inside Databricks Apps. Local execution may require additional Databricks configuration and is not the primary deployment path.

---

## Databricks Data Setup

Create the project catalog and schema:

```sql
CREATE CATALOG IF NOT EXISTS matrimony;

CREATE SCHEMA IF NOT EXISTS matrimony.default;
```

Create the image volume:

```sql
CREATE VOLUME IF NOT EXISTS matrimony.default.profile_images;
```

The Bronze and Gold tables can then be created/populated through the project notebooks.

Authentication tables required by the application are:

```text
matrimony.default.app_users
matrimony.default.app_otp_codes
matrimony.default.app_audit_logs
```

---

## Databricks App Configuration

The application expects the SQL Warehouse to be exposed to the Databricks App as a resource.

A safe `app.yaml` pattern is:

```yaml
command:
  - python
  - app.py

env:
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: sql-warehouse

  - name: SMTP_EMAIL
    value: "your-email@gmail.com"

  - name: SMTP_PASSWORD
    value: "YOUR_SMTP_APP_PASSWORD"
```

### Security Warning

**Never commit real passwords, API keys, tokens, service-principal secrets or other credentials to GitHub.**

For production deployments, store sensitive configuration using the supported Databricks secret/configuration mechanism rather than hard-coding credentials in the repository.

If a credential has already been committed to Git history, treat it as compromised and rotate/revoke it even after deleting it from the current file.

---

## Deploying the Databricks App

A typical deployment flow is:

```bash
databricks apps create matchfinderanalyser \
  --source-code-path /Workspace/Users/<YOUR_EMAIL>/matrimony-match-analyser
```

Add the SQL Warehouse resource:

```bash
databricks apps update matchfinderanalyser \
  --add-resource sql-warehouse:<YOUR_WAREHOUSE_ID>
```

Start the application:

```bash
databricks apps start matchfinderanalyser
```

Check application details:

```bash
databricks apps get matchfinderanalyser
```

---

## Required Permissions

The Databricks App service principal needs appropriate access to the Unity Catalog objects used by the application.

Example:

```sql
GRANT SELECT
ON TABLE matrimony.default.gold_profiles
TO `<APP_SERVICE_PRINCIPAL_ID>`;

GRANT SELECT
ON TABLE matrimony.default.star_compatibility
TO `<APP_SERVICE_PRINCIPAL_ID>`;

GRANT SELECT, MODIFY
ON TABLE matrimony.default.app_users
TO `<APP_SERVICE_PRINCIPAL_ID>`;

GRANT SELECT, MODIFY
ON TABLE matrimony.default.app_otp_codes
TO `<APP_SERVICE_PRINCIPAL_ID>`;

GRANT SELECT, MODIFY
ON TABLE matrimony.default.app_audit_logs
TO `<APP_SERVICE_PRINCIPAL_ID>`;

GRANT READ_VOLUME
ON VOLUME matrimony.default.profile_images
TO `<APP_SERVICE_PRINCIPAL_ID>`;
```

Adjust permissions according to your Databricks governance model.

---

## Application Flow

### New User

```text
Sign Up
   │
   ▼
Create Pending Account
   │
   ▼
Generate OTP
   │
   ▼
Send OTP Email
   │
   ▼
Verify OTP
   │
   ▼
Activate Account
   │
   ▼
Login
```

### Profile Discovery

```text
Login
  │
  ▼
Load Gold Dataset
  │
  ▼
Apply Filters
  │
  ├── Gender
  ├── Birth Year
  ├── Community
  ├── Star
  ├── Rasi
  └── Exclusions
  │
  ▼
Display Matching Profiles
  │
  ▼
View Profile Details
```

### Compatibility

```text
User's Star
    │
    ▼
Compatibility Reference
    │
    ├── Utthamam
    │
    └── Madhyamam
    │
    ▼
Filter Candidate Profiles
```

---

## Security

The project includes several security-related mechanisms.

### Password Protection

Passwords are hashed using:

```text
PBKDF2-HMAC-SHA256
```

with a unique salt per user.

### OTP Verification

OTP-based verification includes:

- Six-digit OTP generation
- Expiration handling
- One-time-use validation
- SMTP delivery

### Databricks Authentication

The application uses Databricks unified authentication and the Databricks App service principal for platform access.

### Unity Catalog

Application data and profile images are stored through Unity Catalog-managed tables and volumes.

### Audit Logging

Authentication events are recorded in:

```text
matrimony.default.app_audit_logs
```

---

## Privacy & Responsible Use

This project processes profile information sourced from a public matrimony website.

Before deploying or distributing the project, consider:

- The source website's Terms of Service
- Robots/extraction policies
- Applicable privacy and data-protection requirements
- Whether personal/contact information should be stored
- Whether profile images should be downloaded or redistributed
- Access controls for application users
- Data retention and deletion policies

**Do not commit scraped profile datasets, personal information, profile images, OTPs, passwords or authentication records to the Git repository.**

For a public GitHub repository, keep the repository focused on the application code and deployment logic rather than the collected personal data.

---

## Development Notes

The project is designed around a clear separation of responsibilities:

```text
Scraping
   ↓
Bronze
   ↓
Transformation
   ↓
Gold
   ↓
Application
```

This makes the project easier to maintain and provides a reusable pattern for other data engineering applications.

The same architecture can be extended to:

- Scheduled ingestion
- Incremental processing
- Additional enrichment
- More compatibility rules
- ML-based recommendations
- Administrative dashboards
- Additional application features

---

## Future Enhancements

Potential future improvements include:

- [ ] Favorites / saved profiles
- [ ] Advanced profile search
- [ ] User preferences
- [ ] In-app messaging
- [ ] Administrative dashboard
- [ ] Additional compatibility dimensions
- [ ] Full traditional horoscope matching
- [ ] Recommendation engine
- [ ] Profile similarity / ML matching
- [ ] Multi-language UI
- [ ] Automated scheduled data refresh
- [ ] CI/CD deployment pipeline
- [ ] Automated data-quality checks
- [ ] Test coverage for scraper and transformation logic

---

## Contributing

Contributions and improvements are welcome.

### Development Workflow

```bash
git checkout -b feature/<feature-name>

git add .

git commit -m "Add <feature description>"

git push origin feature/<feature-name>
```

Then open a Pull Request.

For larger changes, consider opening an issue first to discuss the proposed design.

---

## License

This project is licensed under the MIT License.

See [LICENSE](LICENSE) for details.

---

## Author

**Pradeep**

Data Engineer | Databricks | PySpark | Python | SQL

---

<div align="center">

**Built with Python, PySpark, Delta Lake, and Databricks**

</div>

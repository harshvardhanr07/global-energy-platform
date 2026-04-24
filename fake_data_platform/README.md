# FAKE_DATA_PLATFORM — Technical Specification & Data Design

## Overview

The `fake_data_platform` is a standalone data generation system whose **sole purpose** is to produce realistic, production-like synthetic energy data to feed into the **Global Energy Platform**.

It simulates the exact data sources that would exist in a real-world enterprise energy management system — IoT sensor APIs, vendor invoice CSVs, and a PostgreSQL master data database — complete with intentional imperfections, edge cases, and cross-source consistency rules that mirror real operational data challenges.

---

## Scope

| Dimension | Value |
|---|---|
| Number of Sites | 10 |
| Historical Data Range | 2 years (from today - 2 years to today) |
| Data Generation Mode | Historical batch + continuous near real-time simulation |

---

## Data Sources

There are **3 distinct fake data sources**, each representing a different real-world data system.

---

## Source 1 — APIs (IoT / Sensor Simulation)

### Purpose
Simulates IoT sensors installed on each site that continuously stream electricity consumption and environmental data. In a real deployment, these sensors would be physically installed in server rooms, HVAC units, lighting panels, etc.

### Structure
- **One API endpoint per site** → 10 APIs total
- **Data frequency**: 1 reading per minute per site
- **Coverage**: Full 2-year historical data + ongoing generation

### Data Provided Per API Call

#### Electricity Consumption by Usage Type
Each API returns consumption broken down into **7 usage categories**:

| Usage Type | Description |
|---|---|
| **Heating** | Energy used for space heating systems |
| **Cooling** | Energy used for air conditioning and cooling |
| **Lighting** | Electrical consumption for all lighting systems |
| **Ventilation** | Energy for fans, air handling units, and airflow |
| **UPS** | Uninterruptible power supply system consumption |
| **IT** | Server rooms, networking equipment, workstations |
| **Restaurant** | Kitchen equipment, cafeteria, food service areas |

#### Temperature & Environmental Data
Each API also returns the following environmental metrics:

| Field | Description |
|---|---|
| **Average Outside Temperature** | Real ambient external temperature (°C) |
| **Degree Day Cooling (DDC)** | Measure of how much cooling was needed that day |
| **Degree Day Heating (DDH)** | Measure of how much heating was needed that day |
| **Reference Temperature for Degree Days** | Base temperature used for degree day calculations (typically 18°C) |

### Realism Considerations
- Temperature data should follow seasonal patterns (summer highs, winter lows) and reflect the geographic location of each site (country/city)
- Consumption patterns should reflect occupancy data — lower on weekends, near-zero during site inactive periods
- Minute-level data should contain natural variance and occasional sensor noise/spikes

---

## Source 2 — CSV Files (Invoice / Billing Simulation)

### Purpose
Simulates monthly energy invoices received from vendors. In the real world, these invoices arrive as PDFs. The relevant consumption data is extracted and stored as CSVs on a cloud storage platform (e.g., AWS S3, Azure Blob Storage).

### Structure
- **One CSV file per calendar month**
- Each CSV covers **all 10 sites** for that month
- **Important**: Some sites operate on **mid-month to mid-month billing cycles** (e.g., the 15th of one month to the 14th of the next), not standard calendar months. This creates cross-month alignment complexity in the pipeline.

### CSV Schema

| Column | Type | Description |
|---|---|---|
| `site_id` | String | Unique identifier for the site |
| `site_name` | String | Human-readable name of the site |
| `billing_period_from`|Date|Start date of the billing period (inclusive)|
| `billing_period_to`|Date|End date of the billing period (inclusive)|
| `consumption_type` | String | Type of energy consumed (see below) |
| `consumption` | Float | Total consumption value for the billing period |
| `consumption_unit` | String | Unit of consumption (e.g., kWh, m³, L) |
| `consumption_cost` | Float | Total cost for the billing period |
| `consumption_cost_unit` | String | Currency unit (e.g., USD, EUR, GBP) |
| `cost_per_consumption_unit` | Float | Unit rate (consumption_cost / consumption) |

### Energy Consumption Types

| Type | Description | Unit |
|---|---|---|
| **Electricity** | Grid electricity supply | kWh |
| **Natural Gas** | Piped gas for heating/cooking | m³ or therms |
| **District Energy** | Urban steam or district cooling network | kWh or GJ |
| **Diesel** | On-site diesel generators or fuel | Litres |

### Realism Considerations
- Some months may have missing data for specific sites (simulate late invoices)
- Unit rates may vary month-to-month slightly (simulate tariff changes)
- Mid-to-mid billing cycle sites will appear with different date ranges than calendar-month sites

---

## Source 3 — PostgreSQL Database (Master Data)

### Purpose
Serves as the **source of truth** for all site reference data. This is an operational database that manages the full lifecycle of each site — from activation to closure — and tracks daily occupancy/attendance.

### Tables

---

### Table 1 — `site_profile` (Current Site State)
**Accuracy: 100% — Source of Truth**

Holds the current, up-to-date record for each site.

| Column | Type | Description |
|---|---|---|
| `site_id` | String (PK) | Unique site identifier |
| `site_name` | String | Full site name |
| `status` | Enum | Current status: `active`, `inactive`, `closed`, `colocated` |
| `active_date` | Date | Date the site became operational |
| `inactive_date` | Date | Date the site was decommissioned (null if still active) |
| `country` | String | Country where the site is located |
| `city` | String | City where the site is located |
| `latitude` | Float | Geographic latitude |
| `longitude` | Float | Geographic longitude |
| `site_sqm` | Float | Total site area in square meters |
| `site_capacity` | Integer | Maximum occupancy/headcount for the site |
| `last_updated_on` | Timestamp | Timestamp of last record update |
| `billing_cycle` | Enum | Billing cycle type: calendar (1st–last of month) or mid_month (15th–14th) |
| `timezone` | String | Site local timezone (e.g., Europe/London, America/New_York) |

---

### Table 2 — `site_profile_history` (Audit Trail)
**Accuracy: 100% — Full Audit Log**

Tracks every change ever made to any field in `site_profile`. Enables point-in-time reconstruction of a site's state.

| Column | Type | Description |
|---|---|---|
| `site_id` | String (FK) | Reference to the site |
| `changed_field` | String | Name of the field that was changed |
| `old_value` | String | Previous value before the change |
| `new_value` | String | New value after the change |
| `changed_on` | Timestamp | Exact timestamp the change occurred |

**Example Scenario**: A site moves from London to Manchester → a row is inserted with `changed_field = 'city'`, `old_value = 'London'`, `new_value = 'Manchester'`.

---

### Table 3 — `site_status_history` (Site Lifecycle Events)
**Accuracy: 100% — Lifecycle Tracking**

Records significant lifecycle events for a site — moves, closures, and colocation merges.

| Column | Type | Description |
|---|---|---|
| `site_id` | String (FK) | The site that changed status |
| `event_type` | Enum | Type of event: `moved`, `closed`, `colocated` |
| `new_site_id` | String (nullable) | If moved/colocated, the ID of the destination/new site |
| `updated_on` | Timestamp | Timestamp of when the event was recorded |

**Example Scenario**: Site A is merged into Site B → `event_type = 'colocated'`, `new_site_id = 'SITE_B'`. This enables the pipeline to understand that data from Site A after this date should be attributed to Site B.

---

### Table 4 — `site_occupancy` (Daily Attendance)
**Accuracy: ~97% — Intentional Imperfections**

Tracks how many people attended/occupied each site on a given day. Intentionally contains data quality issues to simulate real-world operational data.

| Column | Type | Description |
|---|---|---|
| `site_id` | String (FK) | Reference to the site |
| `site_capacity` | Integer | Recorded capacity at the time of entry |
| `date` | Date | The date of the occupancy record |
| `occupancy` | Integer | Number of people who attended the site that day |

**Known Data Issues (Intentional)**:
- ~3% of records may be missing, duplicated, or have null values
- Occasional occupancy values exceeding site capacity (data entry errors)
- Some days missing entirely for certain sites

---

## Cross-Source Consistency Rules

This is one of the most important design principles of the platform. Data from the API and the CSV invoices must maintain a defined relationship to enable validation and anomaly detection in the downstream pipeline.

### API vs Invoice Consumption Logic

The API reports granular minute-level consumption. When aggregated over a billing period, it should roughly match the invoice total — but not exactly.

| Scenario | API vs Invoice | Classification | Action |
|---|---|---|---|
| API ≈ Invoice (within ~5%) | Slight variance | ✅ **Normal** | No action |
| API slightly lower than Invoice | API < Invoice by small margin | ✅ **Acceptable** | No action — normal meter lag |
| API significantly lower than Invoice | API << Invoice | 🚨 **Anomaly** | Flag — possible sensor failure or data loss |
| API higher than Invoice | API > Invoice | 🚨 **Anomaly** | Flag — possible double-counting or billing error |

### Why This Matters
This rule drives the entire **data validation and anomaly detection layer** of the Global Energy Platform:
- If API data is always slightly lower → expected (sensor rounding, transmission delays)
- If API data is significantly lower → sensor outage, missing data, or ingestion failure
- If API data is higher than invoiced → billing discrepancy, sensor overcounting, or data corruption

These scenarios should be **generated intentionally** by the fake data platform to ensure the downstream validation pipelines have realistic test cases to work with.

---

## Data Quality Summary

| Source | Expected Accuracy | Intentional Issues |
|---|---|---|
| Site Profile DB | 100% | None — source of truth |
| Site Profile History | 100% | None |
| Site Status History | 100% | None |
| Site Occupancy | ~97% | Missing records, nulls, capacity exceeded |
| API Data | ~95-99% | Sensor noise, occasional spikes, slight mismatch vs invoices |
| CSV Invoices | ~95% | Missing months, mid-to-mid billing cycles, occasional rounding |
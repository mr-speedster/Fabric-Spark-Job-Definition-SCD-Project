# 🚀 Spark Job Definition - SCD Project

A production-style **Slowly Changing Dimension Type 2 (SCD Type 2)** implementation using **PySpark**, designed to run as a **Microsoft Fabric Spark Job Definition**.

This project demonstrates how to structure a Spark application using modular Python files and execute the complete pipeline through a single `main.py` entry point.

---

# 📌 Project Overview

This project implements an end-to-end data processing pipeline using:

- 🐍 Python
- ⚡ PySpark
- 🪣 Microsoft Fabric Lakehouse
- 🔺 Delta Tables
- 🔄 Slowly Changing Dimension Type 2 (SCD2)
- 🏗️ Modular Spark Job Architecture

The project is designed to demonstrate a more production-oriented approach where the Spark logic is separated into multiple modules instead of placing everything inside a single notebook.

The sample schema models a simple **e-commerce** domain (`OrderId`, `PaymentId`, `OrderName`, `PaymentName`, `OrderCount`, ...), and every Delta table/Files location is addressed using an explicit **ABFSS (OneLake) path** built from a Workspace ID, source Lakehouse ID, and target Lakehouse ID supplied at runtime — see [Configure Command Line Arguments](#7️⃣-configure-command-line-arguments).

---

# 🏗️ Project Architecture

```text
GitHub Repository
        │
        ▼
Microsoft Fabric Workspace
        │
        ▼
Spark Job Definition
        │
        ├── Main Definition File
        │       └── main.py
        │
        ├── Reference Files
        │       ├── bronze.py
        │       ├── config.py
        │       ├── data_vault.py
        │       ├── merges.py
        │       ├── pipeline.py
        │       ├── sample_data.py
        │       ├── schema.py
        │       └── watermark.py
        │
        └── Lakehouse Reference
                │
                ▼
        Microsoft Fabric Lakehouse
                │
                ▼
          Delta Tables
```

---

# 📂 Project Structure

The repository contains the following files:

```text
Spark_Job_Definition_SCD_Project_001/
│
├── README.md
│
├── main.py
│
├── bronze.py
│
├── config.py
│
├── data_vault.py
│
├── merges.py
│
├── pipeline.py
│
├── sample_data.py
│
├── schema.py
│
└── watermark.py
```

---

# 📄 File Description

| File | Description |
|---|---|
| `main.py` | Main entry point of the Spark application |
| `config.py` | Contains project-level configuration |
| `sample_data.py` | Creates and manages sample source data |
| `schema.py` | Handles schema-related logic |
| `bronze.py` | Handles Bronze layer processing |
| `data_vault.py` | Contains Data Vault-related transformations |
| `merges.py` | Contains Delta Merge and SCD Type 2 logic |
| `pipeline.py` | Orchestrates the overall data pipeline |
| `watermark.py` | Handles watermark and incremental processing |

---

# ⚙️ Prerequisites

Before running the project, make sure you have access to:

- Microsoft Fabric
- A Fabric Workspace
- A Fabric Lakehouse
- Spark Job Definition capability

---

# 🚀 Steps to Run the Spark Job Definition in Microsoft Fabric

## 1️⃣ Clone or Download the Repository

First, get the project code to your local machine.

You can either:

- Clone the repository using Git.
- Download the repository as a ZIP file and extract it.

Example:

```bash
git clone <repository-url>
```

Alternatively, click:

> **Code → Download ZIP**

Extract the project files before proceeding.

---

## 2️⃣ Open Microsoft Fabric Workspace

Open your **Microsoft Fabric Workspace** where you want to execute the project.

Make sure you have permission to:

- Create a Spark Job Definition
- Create or attach a Lakehouse
- Run Spark workloads

---

## 3️⃣ Create a Lakehouse

Before running the Spark Job, create a Lakehouse.

Navigate to:

> **New Item → Lakehouse**

Give the Lakehouse a suitable name.

For example:

```text
SCD_Project_Lakehouse
```

Once created, this Lakehouse will be used by the Spark Job for reading and writing data.

> 💡 The project uses the default `dbo` schema of the attached Lakehouse.

---

## 4️⃣ Create a Spark Job Definition

Navigate to your Fabric Workspace and select:

> **New Item → Spark Job Definition**

Give the Spark Job Definition a meaningful and unique name.

For example:

```text
Spark_Job_Definition_SCD_Project
```

---

# 🔧 Configure the Spark Job Definition

Once the Spark Job Definition is created, you will see multiple configuration options.

The important configurations for this project are:

- Main Definition File
- Reference Files
- Command Line Arguments
- Lakehouse Reference

---

## 5️⃣ Configure the Main Definition File

The **Main Definition File** is the entry point of the Spark application.

This is where the execution of the project starts.

For this project:

```text
main.py
```

Upload the following file as the **Main Definition File**:

```text
main.py
```

Architecture:

```text
Spark Job Definition
        │
        ▼
      main.py
        │
        ▼
 Pipeline Execution
        │
        ▼
Supporting Modules
```

> ⚠️ Important: `main.py` should be configured only as the Main Definition File.

---

## 6️⃣ Upload Reference Files

The **Reference Files** section is used to upload all supporting Python modules required by the project.

Upload all the remaining Python files except `main.py`.

### Reference Files

```text
bronze.py
config.py
data_vault.py
merges.py
pipeline.py
sample_data.py
schema.py
watermark.py
```

Your configuration should conceptually look like this:

```text
Spark Job Definition
│
├── Main Definition File
│   └── main.py
│
└── Reference Files
    ├── bronze.py
    ├── config.py
    ├── data_vault.py
    ├── merges.py
    ├── pipeline.py
    ├── sample_data.py
    ├── schema.py
    └── watermark.py
```

> ⚠️ Do not upload `main.py` again under Reference Files.

---

## 7️⃣ Configure Command Line Arguments

This project **requires** a command-line argument: the path to a JSON
config file containing the Workspace ID, source Lakehouse ID, and target
Lakehouse ID. These values are used at runtime to build the ABFSS
(OneLake) paths for every Files/Tables location the pipeline reads from
or writes to, instead of relying on the default/direct catalog path.

Create a config file (see `job_config.sample.json` in this repo) with
your own IDs:

```json
{
    "workspaceId": "<your-workspace-id>",
    "sourceLakehouseId": "<lakehouse-id-holding-source-Files>",
    "targetLakehouseId": "<lakehouse-id-to-write-Bronze/Silver-tables>"
}
```

Upload this file to a location the Spark Job can read at runtime (for
example, the `Files` section of an attached Lakehouse), then pass its
path as the Command Line Argument. If you attach a Lakehouse as the
default Lakehouse, its Files are locally mounted, so a typical value
looks like:

```text
Command Line Arguments: /lakehouse/default/Files/job_config.json
```

A direct OneLake path also works, e.g.:

```text
Command Line Arguments: abfss://<workspaceId>@onelake.dfs.fabric.microsoft.com/<lakehouseId>/Files/job_config.json
```

> 💡 Workspace ID and Lakehouse ID (GUIDs) can be copied from the browser
> URL when the Workspace/Lakehouse is open in Microsoft Fabric.

---

## 8️⃣ Attach the Lakehouse

Navigate to the **Lakehouse Reference** section.

Attach the Lakehouse created earlier.

Example:

```text
Lakehouse Reference
        │
        ▼
SCD_Project_Lakehouse
```

The Spark Job will use the attached Lakehouse for processing and storing the output.

The project will use the default schema:

```text
dbo
```

> 💡 Make sure the Lakehouse is properly attached before running the Spark Job.

---

# ✅ Final Configuration Checklist

Before running the job, verify the following:

- [ ] Spark Job Definition is created
- [ ] `main.py` is uploaded as the Main Definition File
- [ ] All supporting Python files are uploaded as Reference Files
- [ ] Command Line Argument is set to the path of your `job_config.json`
- [ ] Source and target Lakehouses exist and their IDs are in the config file
- [ ] Required Lakehouse is attached
- [ ] Spark Job Definition is saved

---

# ▶️ Run the Spark Job

After completing the configuration, save the Spark Job Definition.

You can see the **Run** button in the top toolbar.

Click:

> ▶️ **Run**

Microsoft Fabric will now submit the Spark application for execution.

---

# 🔄 Execution Flow

The high-level execution flow of the project is:

```text
                    ┌─────────────────────┐
                    │      main.py        │
                    │  Application Entry  │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │     pipeline.py     │
                    │ Pipeline Execution  │
                    └──────────┬──────────┘
                               │
                               ▼
              ┌────────────────────────────────┐
              │       Supporting Modules        │
              │                                │
              │  • sample_data.py              │
              │  • schema.py                   │
              │  • bronze.py                   │
              │  • data_vault.py               │
              │  • merges.py                   │
              │  • watermark.py                │
              └───────────────┬────────────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │  Fabric Lakehouse   │
                    │                     │
                    │  Files / Tables     │
                    └─────────────────────┘
```

---

# 📊 Monitor Spark Job Execution

Once the Spark Job starts running, you can monitor its execution from the **Runs** tab.

The **Runs** tab is available at the bottom of the Spark Job Definition page.

You can monitor:

- 🟢 Job Status
- ⏱️ Execution Duration
- 📅 Start Time
- 📅 End Time
- 📜 Spark Logs
- ❌ Error Messages
- 📸 Execution Snapshots
- 🔍 Job Execution Details

---

# 🔍 Verify the Output

After the Spark Job completes successfully:

1. Open the attached Lakehouse.
2. Navigate to the Lakehouse.
3. Check the generated output.

Depending on the pipeline implementation, output can be found under:

### Tables

```text
Lakehouse
└── Tables
    └── dbo
        └── Generated Delta Tables
```

### Files

```text
Lakehouse
└── Files
    └── Generated Files
```

Verify that the expected tables and data have been created successfully.

---

# 🏗️ Complete Deployment Flow

```text
┌───────────────────────────────┐
│       GitHub Repository       │
│                               │
│  Spark Job Definition Project │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│ Clone / Download Repository   │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│ Open Microsoft Fabric         │
│ Workspace                     │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│ Create Lakehouse              │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│ Create Spark Job Definition   │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│ Upload main.py                │
│ as Main Definition File       │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│ Upload Supporting Files       │
│ as Reference Files            │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│ Attach Lakehouse              │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│ Save Spark Job Definition     │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│ ▶️ Run Spark Job              │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│ Monitor Runs Tab              │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│ Verify Lakehouse Output       │
└───────────────────────────────┘
```

---

# 💡 Important Notes

### 1. Entry Point

The `main.py` file acts as the main entry point of the application.

The Spark Job execution starts from this file.

---

### 2. Modular Architecture

Instead of writing the complete Spark application inside a single notebook, the project separates functionality into multiple Python modules.

This provides:

- Better code organization
- Improved maintainability
- Better reusability
- Easier testing
- Cleaner architecture
- Production-friendly development

---

### 3. Reference Files

All supporting Python files must be uploaded as Reference Files so they are available during Spark Job execution.

---

### 4. Lakehouse Dependency

The required Lakehouse must be attached to the Spark Job Definition before execution.

---

### 5. Monitoring

Always check the **Runs** tab after execution.

It provides detailed information about:

- Successful executions
- Failed executions
- Error messages
- Logs
- Execution duration

---

# 🎯 Key Learning

This project demonstrates how to move from a notebook-based Spark development approach towards a more structured and production-oriented architecture.

Instead of:

```text
One Large Notebook
       │
       ▼
Everything Inside Notebook
```

We use:

```text
Spark Job Definition
       │
       ▼
main.py
       │
       ▼
Modular Python Architecture
       │
       ├── Configuration
       ├── Pipeline
       ├── Bronze Layer
       ├── Schema Management
       ├── Merge Logic
       ├── Watermark Logic
       └── Data Vault Logic
```

This approach makes Spark applications easier to maintain, test, version control, and deploy.

---

# 🚀 Happy Learning!

This project is intended as a learning example for understanding:

- Microsoft Fabric Spark Job Definitions
- PySpark project structure
- Modular Python architecture
- Delta Lake processing
- SCD Type 2 implementation
- Data engineering best practices

Feel free to explore, modify, and extend the project! 🎉

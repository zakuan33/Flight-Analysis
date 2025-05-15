# Flight Data Analysis with Apache Spark (Scala)

## Overview

This project analyzes flight data using Apache Spark in Scala,written in IntelliJ IDEA Community. It answers questions based on a flight dataset and writes the output CSV files to a `results/` directory. Questions :

1. Find the total number of flights for each month

2. Find the names of the 100 most frequent flyers. 

3. Find the greatest number of countries a passenger has been in without being in the UK. For example, if the countries a passenger was in were: UK -> FR -> US -> CN -> UK -> DE -> UK, the correct answer would be 3 countries. 

4. Find the passengers who have been on more than 3 flights together. 

5. Find the passengers who have been on more than N flights together within the range (from,to)



## How to Run

### 1. Prerequisites

Ensure the following tools are installed:

- **Java** (JDK 8 or higher)
- **Scala** (2.12.x)
- **Apache Spark** (2.4.x)
- **SBT** (Scala Build Tool)

> ⚠️ If you are using Windows, you must set up `winutils.exe`. See below.

---

### 2. Set Up `winutils.exe` (Windows Only)

Apache Spark on Windows requires `winutils.exe` for certain file system operations (e.g., setting permissions).

#### Steps:

1. Download `winutils.exe` from a trusted source (e.g., https://github.com/steveloughran/winutils).
2. Create the following folder structure:  
   `C:\hadoop\bin\winutils.exe`
3. Set the environment variable `HADOOP_HOME` to `C:\hadoop`
4. Add `%HADOOP_HOME%\bin` to your system `PATH`

---

### 3. Run the Project

1. Open a terminal (PowerShell, CMD, or Terminal).
2. Navigate to the root of the project (where `build.sbt` is located).

```bash
cd path\to\Flight-Analysis
sbt run
```

---

### 4. Output Location

The application will process the data and save the results as CSV files in the following folders within the project:

- `results/question1/`
- `results/question2/`
- `results/question3/`
- `results/question4/`
- `results/question5/`

Each of these folders will contain one CSV file with the output of the corresponding analysis question.

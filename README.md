# 📄 README.md — Marketing Campaign Data Integration Pipeline (SQL)

## 📊 Marketing Campaign Data Integration Pipeline (SQL)

Questa repository contiene una pipeline SQL complessa progettata per unificare, normalizzare e attribuire le performance delle campagne marketing digitali. La query agisce come un layer di trasformazione centrale (ETL) all'interno di un ambiente BigQuery, integrando dati provenienti da piattaforme di CRM, sistemi ERP (SAP), tracciamento Web (Adobe) e tabelle di conversione valuta.

La logica gestisce in modo specifico la transizione tra dati storici (Legacy) e nuovi flussi di dati (post-luglio 2024), garantendo continuità nel reporting.

## 🎯 Obiettivo del progetto

L’obiettivo è generare una "Single Source of Truth" per le performance marketing, risolvendo le discrepanze tra i vari sistemi. La pipeline si occupa di:

*   **Attribuzione Revenue:** Collegare gli ordini di vendita (ERP) alla campagna marketing che li ha generati tramite Tracking Code e Customer Journey.
*   **Normalizzazione Dati:** Standardizzare i formati data e i codici tracciamento (gestione caratteri speciali come `|` vs `_`).
*   **Gestione Valuta:** Applicare i tassi di cambio storici e correnti per normalizzare la revenue.
*   **Segmentazione Temporale:** Gestire la logica di cut-off al `'2024-07-01'` per unire tabelle storiche (`sales_old`, `ds_crm_campaign`) con i flussi attivi.
*   **Recovery "No Cookies":** Integrare conversioni che hanno perso il tracciamento digitale.

Il risultato è un dataset pronto per Dashboard BI che permette di analizzare il ROI per **Brand**, **Country** e **Campaign Type**.

## 🧱 Architettura logica della pipeline

La pipeline integra cinque fonti principali:

| Fonte | Descrizione | Esempi KPI |
| :--- | :--- | :--- |
| **Sales Repository** | Dati transazionali (SAP) storici e correnti | `orders`, `revenue`, `invoice_doc_no` |
| **CRM / Journey** | Metriche di invio e interazione email | `sent`, `delivered`, `opened`, `clicked` |
| **Web Analytics** | Dati di navigazione e unsubscribe (Adobe) | `visits`, `unsubscribe` |
| **Exchange Rates** | Tassi di cambio per anno/country | `cambio` |
| **No Cookie Data** | Conversioni non attribuite digitalmente | `revenue_local`, `orders` |

## 🔄 Diagramma della pipeline

```mermaid
flowchart TD

%% Fonti Dati
A[Sales Data<br/>Old + New Union] --> S[Normalizzazione Vendite<br/>Pre & Post Luglio 24]
B[Exchange Rates<br/>Tassi] --> S
C[Adobe Tracking<br/>Mapping Order-Campagna] --> S

D[CRM Customer Journey<br/>UTM Parsing] --> E[Aggregazione Mensile<br/>Invii e Click]
F[Web Analytics<br/>Visite e Unsub] --> E

%% Logiche di Attribuzione
S --> G[Calcolo Revenue & Attribuzione<br/>Join su Tracking Code]
E --> H[Consolidamento Campagne]

%% Integrazione
H --> I[Master Join (BQ Source)]
G --> I

J[Legacy CRM Data<br/>Dati Storici] --> K[Union All Sources]
I --> K

%% Deduplica e Output
K --> L[Deduplica Avanzata<br/>Priorità Fonte]
M[No Cookies Data] --> N[Output Finale]
L --> N
🧑‍💻 Autore

Leonardo — Data Analyst con 5 anni di esperienza in progetti complessi di integrazione dati, SQL avanzato e pipeline multi‑fonte su Google BigQuery.

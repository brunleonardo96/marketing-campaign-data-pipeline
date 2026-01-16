README.md — Marketing Campaign Data Integration Pipeline (SQL)
📊 Marketing Campaign Data Integration Pipeline (SQL)
Questa repository contiene una pipeline SQL complessa progettata per unificare, normalizzare e attribuire le performance delle campagne marketing digitali. La query agisce come un layer di trasformazione centrale (ETL) all'interno di un ambiente BigQuery, integrando dati provenienti da piattaforme di CRM, sistemi ERP (SAP), tracciamento Web (Adobe) e tabelle di conversione valuta.

La logica gestisce in modo specifico la transizione tra dati storici (Legacy) e nuovi flussi di dati (post-luglio 2024), garantendo continuità nel reporting.

🎯 Obiettivo del progetto
L’obiettivo è generare una "Single Source of Truth" per le performance marketing, risolvendo le discrepanze tra i vari sistemi. La pipeline si occupa di:

Attribuzione Revenue: Collegare gli ordini di vendita (ERP) alla campagna marketing che li ha generati tramite Tracking Code e Customer Journey.
Normalizzazione Dati: Standardizzare i formati data e i codici tracciamento (gestione caratteri speciali come | vs _).
Gestione Valuta: Applicare i tassi di cambio storici e correnti per normalizzare la revenue.
Segmentazione Temporale: Gestire la logica di cut-off al '2024-07-01' per unire tabelle storiche (sales_old, ds_crm_campaign) con i flussi attivi.
Recovery "No Cookies": Integrare conversioni che hanno perso il tracciamento digitale.
Il risultato è un dataset pronto per Dashboard BI che permette di analizzare il ROI per Brand, Country e Campaign Type.

🧱 Architettura logica della pipeline
La pipeline integra cinque fonti principali:

Fonte	Descrizione	Esempi KPI
Sales Repository	Dati transazionali (SAP) storici e correnti	orders, revenue, invoice_doc_no
CRM / Journey	Metriche di invio e interazione email	sent, delivered, opened, clicked
Web Analytics	Dati di navigazione e unsubscribe (Adobe)	visits, unsubscribe
Exchange Rates	Tassi di cambio per anno/country	cambio
No Cookie Data	Conversioni non attribuite digitalmente	revenue_local, orders
🔄 Diagramma della pipeline
mermaid
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
🔄 Principali passaggi della pipeline
1. Preparazione Tassi e Vendite (tassi, pre_sap_aa)
Selezione dei tassi di cambio per l'anno corrente (2025) ed esclusione dei tassi unitari.
Unione (UNION ALL) delle tabelle vendite vecchie e nuove con un punto di taglio al 1° Luglio 2024.
2. Parsing e Pulizia URL (incremental_cj)
Estrazione manuale dei parametri UTM (source, medium, campaign) dagli URL grezzi.
Creazione di un tracking code unificato sostituendo i separatori pipe | con underscore _.
Utilizzo di RANK() per prendere solo l'upload più recente per ogni campagna.
3. Attribuzione Vendite (sap_aa)
Join tra i dati di vendita e la tabella di mapping Adobe (Adobe_mapping_orders_tracking_code).
Classificazione del tipo di transazione: Sub first (prima sottoscrizione), Stand alone, Sub Subsequent (rinnovi).
Conversione della revenue in valuta comune utilizzando la CTE dei tassi.
4. Integrazione Multi-Fonte (all_data)
La CTE all_data è il cuore della logica e unisce:

BQ Source: Dati correnti CRM + Vendite attribuite + Web Data.
OLD Source: Dati storici pre-luglio 2024.
SAP Recurrent: Gestione delle vendite ricorrenti che non hanno una controparte diretta nel CRM mensile corrente.
SAP One Shot: Gestione delle attribuzioni differite (es. email inviata a giugno, acquisto a luglio).
5. Logica di Deduplica (dup_campaigns, all_data_final)
Identificazione di campagne duplicate su stesse date ma fonti diverse.
Prioritizzazione della fonte per evitare il doppio conteggio di revenue o invii (es. azzerando le metriche di una fonte se presente una più affidabile).
6. Mapping Geografico e Brand
Deduzione del Paese e del Brand basata su regole di parsing del nome campagna (es. prefissi 'GB', 'IT', 'US') o della delivery label.
🧠 Tecniche SQL utilizzate
String Parsing Avanzato: Utilizzo combinato di STRPOS, SUBSTRING, LENGTH e REPLACE per decostruire gli URL di tracciamento.
Window Functions: RANK() OVER(PARTITION BY ...) per de-duplicare gli upload dei dati CRM.
Conditional Aggregation: Logiche CASE WHEN complesse per mappare Paesi e Tipi di Transazione.
Date Normalization: CAST(CONCAT(LEFT(..., 8), '01') AS DATE) per riportare tutto al primo del mese.
Full/Left/Right Joins: Strategie di join differenziate per gestire dati mancanti da un lato (es. ordini senza click o click senza ordini).
Group By All: Sfruttamento della sintassi BigQuery per semplificare le aggregazioni.
📈 Output finale
La query produce una tabella con la seguente struttura, pronta per l'ingestione BI:

Ecommerce: Chiave univoca Brand-Country (es. "BRANDX-IT")
Date: Mese di riferimento (1° del mese)
Campaign / Delivery Label: Nome della campagna
Tracking Code: Codice univoco di tracciamento
Type: Canale (EMAIL, SMS, etc.)
Funnel Metrics: delivered, opened, unique_opened, clicked
Sales Metrics: orders, revenue (convertita), revenue_local
Web Metrics: visits, unsubscribe
📂 Struttura della repository
text
/sql
campaign_attribution_logic.sql  <-- La query analizzata
README.md
🔐 Nota sulla privacy
I dati originali non sono inclusi. La query è stata analizzata basandosi sulla logica strutturale. I nomi delle tabelle (es. dataset.repository...) e le logiche di business specifiche sono state rese generiche o anonimizzate per garantire la totale privacy del proprietario dei dati.

🧑‍💻 Autore
Leonardo — Data Analyst con 5 anni di esperienza in progetti complessi di integrazione dati, SQL avanzato e pipeline multi‑fonte su Google BigQuery.
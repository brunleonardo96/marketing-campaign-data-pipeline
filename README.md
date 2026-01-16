# 📊 Marketing Campaign Data Integration Pipeline (SQL)

Questa repository contiene una pipeline SQL avanzata progettata per integrare e normalizzare dati di campagne marketing provenienti da più fonti eterogenee.  
La pipeline unifica informazioni CRM, dati di vendita, tassi di cambio e metriche web, producendo una vista mensile completa delle performance di ogni campagna.

La logica è ispirata a un caso reale, ma **tutti i nomi, le tabelle e le strutture sono stati completamente anonimizzati** per garantire la totale privacy.

---

## 🎯 Obiettivo del progetto

L’obiettivo è creare una **vista unica e coerente** delle performance delle campagne marketing, integrando:

- dati CRM (invii, aperture, click)
- dati web (visite, unsubscribe)
- dati di vendita (ordini, revenue)
- tassi di cambio
- campagne ricorrenti e one‑shot
- deduplica tra sistemi diversi

Il risultato è una tabella finale pronta per:

- dashboard BI  
- analisi mensili  
- attribuzione revenue  
- confronto tra canali e paesi  
- reporting direzionale  

---

## 🧱 Architettura logica della pipeline

La pipeline integra quattro fonti principali:

| Fonte           | Descrizione             | Esempi KPI              |
|-----------------|-------------------------|--------------------------|
| CRM Platform    | invii, aperture, click  | delivered, opened, clicked |
| Web Analytics   | visite, unsubscribe     | visits, unsubscribe     |
| Sales System    | ordini e revenue        | orders, revenue         |
| Exchange Rates  | conversione valuta      | rate                    |

---

## 🔄 Diagramma della pipeline

```mermaid
flowchart TD

    %% Fonti principali
    A[CRM Platform<br/>Invii, aperture, click] --> D[Normalizzazione Tracking Code]
    B[Web Analytics<br/>Visite, unsubscribe] --> D
    C[Sales System<br/>Ordini, revenue] --> E[Integrazione Vendite]
    F[Exchange Rates<br/>Tassi di cambio] --> E

    %% Step intermedi
    D --> G[Deduplica CRM<br/>Window Functions]
    G --> H[Unione CRM Storico + CRM Moderno]

    E --> I[Conversione Valuta<br/>Aggregazione Mensile]
    I --> J[Attribuzione Revenue<br/>First/Repeat Purchase]

    %% Unione finale
    H --> K[Unione Multi-Fonte]
    J --> K
    B --> K

    %% Output finale
    K --> L[Vista Finale Campagne<br/>KPI CRM + Sales + Web]

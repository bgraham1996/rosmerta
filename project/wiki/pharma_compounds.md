# Pharmaceutical Compound Reference

A working reference of pharmaceutical **compounds** (marketed drugs + material clinical
pipeline) relevant to the asset prices of the pharma/biotech companies represented in the
rosmerta book, plus the direct competitor compounds that move those prices.

> **This list is a research aid, not investment advice, and is intentionally _not_ stored in
> the database** (per request — Markdown wiki only).

## Scope & method

- **Universe:** anchored on the pharma/biotech tickers in the `stocks` table (LLY, MRK, PFE,
  JNJ, ABBV, BMY, AMGN, GILD, AZN, GSK, NVS, NVO, RHHBY, BIIB, REGN, MRNA, BNTX, INCY, EXEL,
  JAZZ, APGE, IBRX, VTRS, ZTS), **plus** key external competitor/partner compounds that
  materially move those prices (Sanofi, Vertex, Argenx, GLP-1 challengers, biosimilar makers,
  etc.).
- **Status coverage:** approved/marketed products **and** clinically material pipeline
  (Phase 2/3, filed/PDUFA, near-term catalysts). Selected patent-cliff / lost-exclusivity
  (LOE) compounds are included where the cliff still shapes valuation.
- **Fields (minimal, as requested):** `Compound` · `Class` (drug class / target / mechanism)
  · `Status`. **Owner** is the section heading. Brand and generic (INN) names are given
  together as `Brand (generic)` where both exist; pipeline entries use the code or INN.
- **Pure device / tools / distribution tickers make no compounds** (DXCM, MDT, ABT, BDX, ALC,
  ILMN, TWST, RGEN, GEHC, TMO, DHR, A, CVS) — see [Appendix B](#appendix-b--non-compound-tickers-in-the-book).

### Sourcing & confidence

Built primarily from model knowledge current to **~Jan 2026**, with a recency top-up from FDA
CDER 2025 approvals and 2025–2026 pipeline coverage (web-verified spot items include Lilly
*Foundayo*/orforglipron approval, Pfizer/Arvinas *Veppanu*/vepdegestrant, AbbVie *PVEK*/
pivekimab, Merck doravirine/islatravir). **Marketed rows are high-confidence; pipeline codes,
phase, and status are approximate and time-sensitive — verify against Drugs@FDA / company
pipeline pages / ClinicalTrials.gov before any trading use.**

### Status legend

| Tag | Meaning |
|-----|---------|
| **Mkt** | Marketed / FDA-approved (or approved in a major market) |
| **Filed** | NDA/BLA/sBLA filed or under review (incl. PDUFA pending) |
| **Ph3** | Pivotal / Phase 3 |
| **Ph2** | Phase 2 |
| **Ph1** | Phase 1 / early |
| **LOE** | Marketed but past/near loss of exclusivity — patent-cliff risk |
| **Disc** | Discontinued / withdrawn (kept for cliff/valuation history) |
| **Bx** | Biosimilar |

---

## Eli Lilly (LLY)

| Compound | Class | Status |
|----------|-------|--------|
| Mounjaro (tirzepatide) | GIP/GLP-1 dual agonist | Mkt |
| Zepbound (tirzepatide) | GIP/GLP-1 dual agonist (obesity) | Mkt |
| Trulicity (dulaglutide) | GLP-1 agonist | Mkt/LOE-approaching |
| Foundayo (orforglipron) | Oral non-peptide GLP-1 agonist | Mkt (2026) |
| Retatrutide | GIP/GLP-1/glucagon triple agonist | Ph3 |
| Jardiance (empagliflozin) | SGLT2 inhibitor (w/ Boehringer) | Mkt |
| Verzenio (abemaciclib) | CDK4/6 inhibitor | Mkt |
| Kisunla (donanemab) | Anti-amyloid (Alzheimer's) mAb | Mkt |
| Taltz (ixekizumab) | IL-17A inhibitor | Mkt |
| Ebglyss (lebrikizumab) | IL-13 inhibitor (atopic derm) | Mkt |
| Omvoh (mirikizumab) | IL-23 inhibitor (IBD) | Mkt |
| Jaypirca (pirtobrutinib) | Non-covalent BTK inhibitor | Mkt |
| Cyramza (ramucirumab) | Anti-VEGFR2 mAb | Mkt |
| Emgality (galcanezumab) | Anti-CGRP mAb (migraine) | Mkt |
| Reyvow (lasmiditan) | 5-HT1F agonist (migraine) | Mkt |
| Olumiant (baricitinib) | JAK1/2 inhibitor | Mkt |
| Retevmo (selpercatinib) | RET kinase inhibitor | Mkt |
| Alimta (pemetrexed) | Antifolate chemo | Mkt/LOE |
| Basaglar (insulin glargine) | Basal insulin (Bx of Lantus) | Mkt/Bx |
| Humalog (insulin lispro) | Rapid-acting insulin | Mkt/LOE |
| Lyumjev (insulin lispro-aabc) | Ultra-rapid insulin | Mkt |
| Muvalaplin | Oral Lp(a) inhibitor | Ph2 |
| Lepodisiran | Lp(a) siRNA | Ph3 |
| Bimagrumab | Anti-ActRII mAb (muscle-preserving, obesity) | Ph2 |
| Eloralintide | Amylin receptor agonist (obesity) | Ph2 |
| Mazdutide | GLP-1/glucagon dual (China rights w/ Innovent) | Ph3 |
| Imlunestrant | Oral SERD (breast cancer) | Filed/Ph3 |
| Olomorasib | KRAS G12C inhibitor (2nd gen) | Ph2 |
| Tibulizumab | Dual BAFF/IL-17 | Ph2 |
| Peresolimab | PD-1 agonist mAb (RA) | Ph2 |
| Remternetug | Anti-amyloid (SC, Alzheimer's) | Ph3 |
| Solanezumab | Anti-amyloid (failed prevention) | Disc |
| Volenrelaxin | Relaxin analog (heart failure) | Ph2 |
| Empagliflozin CKD/HF | SGLT2 | Mkt |
| Baricitinib alopecia/SLE | JAK1/2 | Mkt |
| Donanemab prevention (TRAILBLAZER-3) | Anti-amyloid | Ph3 |
| Tirzepatide MASH/HFpEF | GIP/GLP-1 | Ph3 |
| Orforglipron (T2D) | Oral GLP-1 | Filed |
| Verzenio adjuvant (early breast) | CDK4/6 | Mkt |
| Jaypirca CLL/MCL | BTK inhibitor | Mkt |
| Lebrikizumab (COPD/other) | IL-13 | Ph2 |
| Ceralasertib — see AZN | — | — |
| Cyltezo — see biosimilars | — | — |

## Merck & Co (MRK)

| Compound | Class | Status |
|----------|-------|--------|
| Keytruda (pembrolizumab) | Anti-PD-1 mAb | Mkt (LOE ~2028) |
| Keytruda Qlex (subcutaneous pembro) | Anti-PD-1 SC + berahyaluronidase | Mkt/Filed |
| Gardasil / Gardasil 9 | HPV vaccine | Mkt |
| Januvia / Janumet (sitagliptin) | DPP-4 inhibitor | Mkt/LOE |
| Bridion (sugammadex) | Neuromuscular blockade reversal | Mkt/LOE |
| Lynparza (olaparib) | PARP inhibitor (w/ AZN) | Mkt |
| Lenvima (lenvatinib) | Multi-kinase inhibitor (w/ Eisai) | Mkt |
| Welireg (belzutifan) | HIF-2α inhibitor | Mkt |
| Winrevair (sotatercept) | Activin signaling inhibitor (PAH) | Mkt |
| Capvaxive | 21-valent pneumococcal vaccine | Mkt |
| Vaxneuvance | 15-valent pneumococcal vaccine | Mkt |
| Lagevrio (molnupiravir) | Oral antiviral (COVID) | Mkt |
| Isentress (raltegravir) | HIV integrase inhibitor | Mkt/LOE |
| Belsomra (suvorexant) | Orexin antagonist (insomnia) | Mkt |
| Prevymis (letermovir) | CMV antiviral | Mkt |
| Enflonsia (clesrovimab) | Anti-RSV mAb (infant) | Mkt/Filed |
| Enlicitide (MK-0616) | Oral PCSK9 inhibitor | Ph3 |
| Doravirine/islatravir | HIV NNRTI + NRTTI combo | Filed |
| Islatravir combos (HIV) | Nucleoside RT translocation inhibitor | Ph3 |
| Sacituzumab tirumotecan (sac-TMT, MK-2870) | TROP2 ADC (w/ Kelun) | Ph3 |
| Bomedemstat | LSD1 inhibitor (MPN) | Ph3 |
| Nemtabrutinib | Non-covalent BTK inhibitor | Ph3 |
| Zilovertamab vedotin | ROR1 ADC | Ph2 |
| V940 / mRNA-4157 (intismeran) | Individualized neoantigen (w/ Moderna) | Ph3 |
| Tulisokibart (MK-7240) | Anti-TL1A mAb (IBD) | Ph3 |
| Quavonlimab | Anti-CTLA-4 | Ph2 |
| Vibostolimab | Anti-TIGIT (w/ pembro) | Ph2 |
| Favezelimab | Anti-LAG-3 | Ph3 |
| MK-1084 | KRAS G12C inhibitor | Ph2 |
| Sotatercept (expanded PAH) | Activin trap | Mkt |
| Belzutifan expansions | HIF-2α | Mkt |
| Pembrolizumab perioperative combos | Anti-PD-1 | Mkt |
| MK-0616 CVOT | Oral PCSK9 | Ph3 |
| V116 (Capvaxive) adult | Pneumococcal | Mkt |
| Patritumab deruxtecan — see Daiichi | — | — |
| Ifinatamab deruxtecan — see Daiichi | — | — |
| Gefurulimab — see AZN | — | — |

## Pfizer (PFE)

| Compound | Class | Status |
|----------|-------|--------|
| Comirnaty (tozinameran) | COVID mRNA vaccine (w/ BioNTech) | Mkt |
| Paxlovid (nirmatrelvir/ritonavir) | Oral antiviral (COVID) | Mkt |
| Eliquis (apixaban) | Factor Xa inhibitor (w/ BMS) | Mkt (LOE ~2028) |
| Prevnar 20 / Prevnar 13 | Pneumococcal conjugate vaccine | Mkt |
| Ibrance (palbociclib) | CDK4/6 inhibitor | Mkt (LOE approaching) |
| Xtandi (enzalutamide) | Androgen receptor inhibitor (w/ Astellas) | Mkt |
| Vyndaqel / Vyndamax (tafamidis) | TTR stabilizer (ATTR-CM) | Mkt |
| Padcev (enfortumab vedotin) | Nectin-4 ADC (w/ Astellas) | Mkt |
| Adcetris (brentuximab vedotin) | CD30 ADC | Mkt |
| Braftovi (encorafenib) | BRAF inhibitor | Mkt |
| Mektovi (binimetinib) | MEK inhibitor | Mkt |
| Nurtec ODT / Zavzpret (rimegepant / zavegepant) | CGRP antagonist (migraine) | Mkt |
| Litfulo (ritlecitinib) | JAK3/TEC inhibitor (alopecia) | Mkt |
| Cibinqo (abrocitinib) | JAK1 inhibitor (atopic derm) | Mkt |
| Velsipity (etrasimod) | S1P modulator (UC) | Mkt |
| Elrexfio (elranatamab) | BCMA×CD3 bispecific (myeloma) | Mkt |
| Abrysvo | RSV vaccine (maternal/older adults) | Mkt |
| Ngenla (somatrogon) | Long-acting growth hormone | Mkt |
| Talzenna (talazoparib) | PARP inhibitor | Mkt |
| Lorbrena (lorlatinib) | ALK inhibitor | Mkt |
| Bosulif (bosutinib) | BCR-ABL TKI | Mkt |
| Inlyta (axitinib) | VEGFR TKI | Mkt |
| Sutent (sunitinib) | Multi-kinase TKI | Mkt/LOE |
| Hympavzi (marstacimab) | Anti-TFPI (hemophilia) | Mkt |
| Beqvez (fidanacogene elaparvovec) | Hemophilia B gene therapy | Mkt |
| Oxbryta (voxelotor) | HbS polymerization inhibitor (sickle cell) | Disc (withdrawn) |
| Veppanu (vepdegestrant) | ER PROTAC degrader (w/ Arvinas) | Mkt (2026) |
| Sigvotatug vedotin | Integrin β6 ADC | Ph3 |
| Atirmociclib (PF-07220060) | CDK4-selective inhibitor | Ph3 |
| Danuglipron | Oral small-molecule GLP-1 agonist | Disc (2025) |
| PF-07985045 | Oral GIPR antagonist (obesity) | Ph2 |
| Ponsegromab | Anti-GDF-15 (cachexia) | Ph3 |
| Fordadistrogene movaparvovec | DMD mini-dystrophin AAV | Disc/Ph3 |
| Braftovi + cetuximab CRC | BRAF combo | Mkt |
| Sasanlimab | Anti-PD-1 (NMIBC, w/ BCG) | Filed |
| Mevrometostat | EZH2 inhibitor (prostate) | Ph3 |
| Comirnaty combo (COVID/flu) | mRNA vaccine (w/ BioNTech) | Ph3 |
| Prevnar (higher valency) | Pneumococcal | Ph3 |
| Abrysvo (younger adults) | RSV vaccine | Filed |
| Emblaveo (aztreonam/avibactam) | β-lactam/β-lactamase inhibitor | Mkt |
| Elranatamab earlier lines | BCMA bispecific | Ph3 |
| Zithromax/Lipitor/Lyrica (legacy) | — | LOE |

## Johnson & Johnson (JNJ)

| Compound | Class | Status |
|----------|-------|--------|
| Stelara (ustekinumab) | IL-12/23 inhibitor | Mkt/LOE (biosimilars live) |
| Tremfya (guselkumab) | IL-23 inhibitor | Mkt |
| Darzalex / Darzalex Faspro (daratumumab) | Anti-CD38 mAb (myeloma) | Mkt |
| Erleada (apalutamide) | Androgen receptor inhibitor | Mkt |
| Imbruvica (ibrutinib) | BTK inhibitor (w/ AbbVie) | Mkt/declining |
| Carvykti (cilta-cel) | BCMA CAR-T (w/ Legend) | Mkt |
| Tecvayli (teclistamab) | BCMA×CD3 bispecific | Mkt |
| Talvey (talquetamab) | GPRC5D×CD3 bispecific | Mkt |
| Rybrevant (amivantamab) | EGFR×MET bispecific | Mkt |
| Lazcluze (lazertinib) | 3rd-gen EGFR TKI (w/ Rybrevant) | Mkt |
| Uptravi (selexipag) | Prostacyclin agonist (PAH) | Mkt |
| Opsumit (macitentan) | Endothelin antagonist (PAH) | Mkt |
| Xarelto (rivaroxaban) | Factor Xa inhibitor (w/ Bayer) | Mkt/LOE |
| Invega Sustenna/Trinza/Hafyera (paliperidone) | Long-acting antipsychotic | Mkt |
| Spravato (esketamine) | NMDA antagonist (depression) | Mkt |
| Balversa (erdafitinib) | FGFR inhibitor | Mkt |
| Akeega (niraparib + abiraterone) | PARP + AR combo | Mkt |
| Ponvory (ponesimod) | S1P modulator (MS) | Mkt |
| Simponi / Simponi Aria (golimumab) | Anti-TNF | Mkt |
| Remicade (infliximab) | Anti-TNF | Mkt/LOE |
| Imaavy (nipocalimab) | FcRn inhibitor | Mkt/Filed |
| Icotrokinra (JNJ-2113) | Oral IL-23 receptor peptide | Ph3/Filed |
| Milvexian | Oral Factor XIa inhibitor (w/ BMS) | Ph3 |
| TAR-200 | Intravesical gemcitabine (bladder) | Filed |
| Bleximenib | Menin inhibitor | Ph2 |
| Pasritamig (JNJ-5322) | KLK2×CD3 (prostate) | Ph1/2 |
| Aticaprant | Kappa-opioid antagonist (MDD) | Disc/Ph3 |
| Seltorexant | Orexin-2 antagonist (MDD + insomnia) | Ph3/Filed |
| Guselkumab subQ IBD | IL-23 | Mkt |
| Amivantamab + lazertinib (MARIPOSA) | EGFR combos | Mkt |
| Talquetamab combos | GPRC5D | Ph3 |
| Daratumumab frontline/smoldering | Anti-CD38 | Mkt/Ph3 |
| Posdinemab | Anti-tau (Alzheimer's) | Ph2 |
| Rybrevant SC | EGFR×MET bispecific SC | Mkt |
| Tremfya IBD | IL-23 (UC/Crohn's) | Mkt |

## AbbVie (ABBV)

| Compound | Class | Status |
|----------|-------|--------|
| Humira (adalimumab) | Anti-TNF mAb | Mkt/LOE (biosimilars live) |
| Skyrizi (risankizumab) | IL-23 inhibitor | Mkt |
| Rinvoq (upadacitinib) | JAK1 inhibitor | Mkt |
| Venclexta (venetoclax) | BCL-2 inhibitor (w/ Roche) | Mkt |
| Imbruvica (ibrutinib) | BTK inhibitor (w/ JNJ) | Mkt/declining |
| Vraylar (cariprazine) | D3/D2 partial agonist (antipsychotic) | Mkt |
| Botox (onabotulinumtoxinA) | Neurotoxin (therapeutic + aesthetic) | Mkt |
| Juvederm | Dermal filler (hyaluronic acid) | Mkt |
| Ubrelvy (ubrogepant) | Oral CGRP antagonist (acute migraine) | Mkt |
| Qulipta / Aquipta (atogepant) | Oral CGRP antagonist (prevention) | Mkt |
| Epkinly (epcoritamab) | CD3×CD20 bispecific (w/ Genmab) | Mkt |
| Elahere (mirvetuximab soravtansine) | FRα ADC (ovarian) | Mkt |
| Emrelis (telisotuzumab vedotin) | c-Met ADC (NSCLC) | Mkt |
| Mavyret (glecaprevir/pibrentasvir) | HCV antiviral combo | Mkt |
| Vyalev (foscarbidopa/foslevodopa) | Subcutaneous levodopa (Parkinson's) | Mkt |
| Linzess (linaclotide) | GC-C agonist (IBS-C, w/ Ironwood) | Mkt |
| Creon (pancrelipase) | Pancreatic enzymes | Mkt |
| Ozurdex / Durysta | Ophthalmic implants | Mkt |
| PVEK (pivekimab sunirine) | CD123 ADC (BPDCN) | Filed |
| Tavapadon | D1/D5 partial agonist (Parkinson's) | Ph3/Filed |
| Emraclidine | M4 muscarinic agonist (schizophrenia) | Disc (2024 Ph2 fail) |
| Lutikizumab | Anti-IL-1α/β (HS, IBD) | Ph2 |
| ABBV-383 (etentamig) | BCMA×CD3 bispecific (myeloma) | Ph3 |
| Budigalimab | Anti-PD-1 | Ph2 |
| ABBV-706 | SEZ6 ADC (SCLC) | Ph1 |
| ABBV-400 (temab-A) | c-Met TOP1 ADC (CRC) | Ph2 |
| Skyrizi HS / UC / CD | IL-23 | Mkt |
| Rinvoq (GCA, vitiligo, etc.) | JAK1 | Mkt/Ph3 |
| Epcoritamab earlier lines | CD3×CD20 | Ph3 |
| Teliso-V c-Met adnectin | c-Met | Ph1 |

## Bristol Myers Squibb (BMY)

| Compound | Class | Status |
|----------|-------|--------|
| Eliquis (apixaban) | Factor Xa inhibitor (w/ Pfizer) | Mkt (LOE ~2028) |
| Opdivo (nivolumab) | Anti-PD-1 mAb | Mkt |
| Opdivo Qvantig (nivolumab SC) | Anti-PD-1 SC + hyaluronidase | Mkt |
| Revlimid (lenalidomide) | Immunomodulator (IMiD) | Mkt/LOE (generics) |
| Reblozyl (luspatercept) | Erythroid maturation agent | Mkt |
| Pomalyst (pomalidomide) | IMiD | Mkt |
| Sprycel (dasatinib) | BCR-ABL TKI | Mkt/LOE |
| Yervoy (ipilimumab) | Anti-CTLA-4 mAb | Mkt |
| Orencia (abatacept) | CTLA-4-Ig (T-cell costim) | Mkt |
| Camzyos (mavacamten) | Cardiac myosin inhibitor (HCM) | Mkt |
| Sotyktu (deucravacitinib) | TYK2 inhibitor (psoriasis) | Mkt |
| Breyanzi (liso-cel) | CD19 CAR-T | Mkt |
| Abecma (ide-cel) | BCMA CAR-T (w/ 2seventy) | Mkt |
| Zeposia (ozanimod) | S1P modulator (MS, UC) | Mkt |
| Opdualag (nivolumab/relatlimab) | PD-1 + LAG-3 combo | Mkt |
| Cobenfy (xanomeline/trospium) | M1/M4 muscarinic agonist (schizophrenia) | Mkt |
| Augtyro (repotrectinib) | ROS1/TRK inhibitor | Mkt |
| Krazati (adagrasib) | KRAS G12C inhibitor | Mkt |
| Inrebic (fedratinib) | JAK2 inhibitor (myelofibrosis) | Mkt |
| Onureg (oral azacitidine) | Hypomethylating agent | Mkt |
| Milvexian | Oral Factor XIa inhibitor (w/ JNJ) | Ph3 |
| Cendakimab | Anti-IL-13 (eosinophilic esophagitis) | Filed/Ph3 |
| Iberdomide | CELMoD (myeloma) | Ph3 |
| Mezigdomide | CELMoD (myeloma) | Ph3 |
| Golcadomide | CELMoD (lymphoma) | Ph3 |
| Cobenfy adjunct (Alzheimer psychosis / schizo adjunct) | Muscarinic | Ph3 |
| Camzyos (non-obstructive HCM) | Myosin inhibitor | Ph3 |
| Alnuctamab | BCMA×CD3 bispecific | Ph2 |
| BMS-986365 (CC-94676) | AR ligand-directed degrader (prostate) | Ph2 |
| Farletuzumab ecteribulin | FRα ADC (w/ Eisai) | Ph2 |
| Deucravacitinib (PsA, SLE, IBD) | TYK2 | Ph3 |
| Repotrectinib expansions | ROS1/TRK | Mkt |
| Subcutaneous nivolumab expansions | Anti-PD-1 | Mkt |

## Amgen (AMGN)

| Compound | Class | Status |
|----------|-------|--------|
| Enbrel (etanercept) | TNF inhibitor (fusion protein) | Mkt/LOE |
| Prolia (denosumab) | Anti-RANKL (osteoporosis) | Mkt/LOE (biosimilars) |
| Xgeva (denosumab) | Anti-RANKL (oncology) | Mkt/LOE |
| Repatha (evolocumab) | Anti-PCSK9 mAb | Mkt |
| Otezla (apremilast) | PDE4 inhibitor (psoriasis) | Mkt |
| Tepezza (teprotumumab) | Anti-IGF-1R (thyroid eye disease) | Mkt |
| Krystexxa (pegloticase) | Uricase (chronic gout) | Mkt |
| Blincyto (blinatumomab) | CD19×CD3 BiTE | Mkt |
| Kyprolis (carfilzomib) | Proteasome inhibitor | Mkt |
| Nplate (romiplostim) | TPO receptor agonist | Mkt |
| Vectibix (panitumumab) | Anti-EGFR mAb | Mkt |
| Aimovig (erenumab) | Anti-CGRP receptor mAb | Mkt |
| Tavneos (avacopan) | C5a receptor antagonist (ANCA vasculitis) | Mkt |
| Lumakras (sotorasib) | KRAS G12C inhibitor | Mkt |
| Tezspire (tezepelumab) | Anti-TSLP (asthma, w/ AZN) | Mkt |
| Uplizna (inebilizumab) | Anti-CD19 (NMOSD, IgG4-RD) | Mkt |
| Imdelltra (tarlatamab) | DLL3×CD3 bispecific (SCLC) | Mkt |
| Bkemv (eculizumab-aeeb) | C5 inhibitor biosimilar (Soliris) | Mkt/Bx |
| Wezlana (ustekinumab-auub) | Stelara biosimilar | Mkt/Bx |
| Pavblu (aflibercept-ayyh) | Eylea biosimilar | Mkt/Bx |
| Amjevita (adalimumab-atto) | Humira biosimilar | Mkt/Bx |
| MariTide (maridebart cafraglutide) | GIPR antagonist / GLP-1 agonist (obesity) | Ph3 |
| Olpasiran | Lp(a) siRNA | Ph3 |
| Rocatinlimab | Anti-OX40 (atopic derm) | Ph3 |
| Bemarituzumab | Anti-FGFR2b (gastric) | Ph3/Filed |
| Xaluritamig (AMG 509) | STEAP1×CD3 (prostate) | Ph3 |
| AMG 193 | MTA-cooperative PRMT5 inhibitor | Ph2 |
| Blincyto frontline ALL | CD19 BiTE | Mkt |
| Tarlatamab 1L / combos | DLL3 bispecific | Ph3 |
| Sotorasib + panitumumab CRC | KRAS G12C combo | Mkt |
| Tepezza subcutaneous | Anti-IGF-1R SC | Ph3 |

## Gilead Sciences (GILD)

| Compound | Class | Status |
|----------|-------|--------|
| Biktarvy (BIC/FTC/TAF) | HIV integrase-based triple | Mkt |
| Descovy (FTC/TAF) | HIV NRTI / PrEP | Mkt |
| Genvoya / Odefsey / Stribild | HIV single-tablet regimens | Mkt |
| Sunlenca (lenacapavir) | HIV capsid inhibitor (long-acting) | Mkt |
| Yeztugo (lenacapavir PrEP) | HIV capsid inhibitor (twice-yearly PrEP) | Mkt (2025) |
| Veklury (remdesivir) | Antiviral (COVID) | Mkt |
| Yescarta (axi-cel) | CD19 CAR-T | Mkt |
| Tecartus (brexu-cel) | CD19 CAR-T | Mkt |
| Trodelvy (sacituzumab govitecan) | TROP2 ADC | Mkt |
| Livdelzi (seladelpar) | PPARδ agonist (PBC) | Mkt |
| Vemlidy (tenofovir alafenamide) | HBV antiviral | Mkt |
| Epclusa (sofosbuvir/velpatasvir) | HCV antiviral | Mkt |
| Harvoni / Sovaldi | HCV antivirals | Mkt/LOE |
| Hepcludex (bulevirtide) | HDV entry inhibitor | Mkt (EU) |
| Letairis (ambrisentan) | Endothelin antagonist | Mkt/LOE |
| Trodelvy 1L HR+/HER2- (combos) | TROP2 ADC | Ph3 |
| Anito-cel (anitocabtagene) | BCMA CAR-T (w/ Arcellx) | Ph3/Filed |
| Lenacapavir + islatravir (weekly oral) | HIV combo (w/ Merck) | Ph2 |
| Domvanalimab | Anti-TIGIT (w/ Arcus) | Ph3 |
| Zimberelimab | Anti-PD-1 (w/ Arcus) | Ph3 |
| Trodelvy + pembro | TROP2 ADC combo | Ph3 |
| GS-5245 (obeldesivir) | Oral antiviral | Ph3 |
| Seladelpar expansions | PPARδ | Ph3 |
| Lenacapavir (twice-yearly HIV treatment) | Capsid inhibitor | Ph3 |
| Magrolimab (anti-CD47) | Macrophage checkpoint | Disc (2024) |

## AstraZeneca (AZN)

| Compound | Class | Status |
|----------|-------|--------|
| Tagrisso (osimertinib) | 3rd-gen EGFR TKI | Mkt |
| Imfinzi (durvalumab) | Anti-PD-L1 mAb | Mkt |
| Farxiga / Forxiga (dapagliflozin) | SGLT2 inhibitor | Mkt |
| Calquence (acalabrutinib) | BTK inhibitor | Mkt |
| Lynparza (olaparib) | PARP inhibitor (w/ Merck) | Mkt |
| Enhertu (trastuzumab deruxtecan) | HER2 ADC (w/ Daiichi Sankyo) | Mkt |
| Fasenra (benralizumab) | Anti-IL-5R (eosinophilic asthma) | Mkt |
| Symbicort (budesonide/formoterol) | ICS/LABA inhaler | Mkt/LOE |
| Breztri (BGF triple) | ICS/LAMA/LABA (COPD) | Mkt |
| Ultomiris (ravulizumab) | C5 inhibitor (long-acting) | Mkt |
| Soliris (eculizumab) | C5 inhibitor | Mkt/LOE |
| Saphnelo (anifrolumab) | Anti-IFNAR1 (SLE) | Mkt |
| Beyfortus (nirsevimab) | Anti-RSV mAb (w/ Sanofi) | Mkt |
| Airsupra (albuterol/budesonide) | Rescue ICS/SABA | Mkt |
| Wainua (eplontersen) | TTR antisense (ATTR-PN, w/ Ionis) | Mkt |
| Truqap (capivasertib) | AKT inhibitor (breast) | Mkt |
| Datroway (datopotamab deruxtecan) | TROP2 ADC (w/ Daiichi) | Mkt |
| Koselugo (selumetinib) | MEK inhibitor (NF1) | Mkt |
| Voydeya (danicopan) | Factor D inhibitor (PNH add-on) | Mkt |
| Imjudo (tremelimumab) | Anti-CTLA-4 | Mkt |
| Tezspire (tezepelumab) | Anti-TSLP (w/ Amgen) | Mkt |
| Camizestrant | Oral SERD (breast) | Filed/Ph3 |
| Baxdrostat | Aldosterone synthase inhibitor (HTN) | Ph3 |
| Gefurulimab | Anti-C5 nanobody (gMG) | Ph3 |
| Volrustomig | PD-1×CTLA-4 bispecific | Ph3 |
| Rilvegostomig | PD-1×TIGIT bispecific | Ph3 |
| Tozorakimab | Anti-IL-33 (COPD) | Ph3 |
| Saruparib (AZD5305) | PARP1-selective inhibitor | Ph3 |
| Anselamimab (CAEL-101) | Anti-amyloid (AL amyloidosis) | Ph3 |
| AZD0780 | Oral PCSK9 inhibitor | Ph2 |
| Enhertu HER2-low/ultralow | HER2 ADC | Mkt |
| Datopotamab (lung/breast) | TROP2 ADC | Filed |
| Tagrisso adjuvant/unresectable | EGFR TKI | Mkt |
| Dato-DXd + durvalumab | ADC + IO combo | Ph3 |

## GSK (GSK)

| Compound | Class | Status |
|----------|-------|--------|
| Shingrix | Recombinant zoster (shingles) vaccine | Mkt |
| Arexvy | RSV vaccine (older adults) | Mkt |
| Trelegy Ellipta (FF/UMEC/VI) | ICS/LAMA/LABA triple | Mkt |
| Nucala (mepolizumab) | Anti-IL-5 (eosinophilic disease) | Mkt |
| Benlysta (belimumab) | Anti-BLyS/BAFF (SLE) | Mkt |
| Dovato (dolutegravir/lamivudine) | HIV two-drug regimen | Mkt |
| Triumeq (dolutegravir/ABC/3TC) | HIV single-tablet | Mkt |
| Cabenuva (cabotegravir/rilpivirine) | Long-acting injectable HIV | Mkt |
| Apretude (cabotegravir LA) | HIV PrEP (injectable) | Mkt |
| Jemperli (dostarlimab) | Anti-PD-1 (endometrial) | Mkt |
| Zejula (niraparib) | PARP inhibitor | Mkt |
| Blenrep (belantamab mafodotin) | BCMA ADC (myeloma) | Mkt (relaunch) |
| Ojjaara / Omjjara (momelotinib) | JAK1/2 + ACVR1 (myelofibrosis) | Mkt |
| Bexsero / Menveo | Meningococcal vaccines | Mkt |
| Rukobia (fostemsavir) | HIV attachment inhibitor | Mkt |
| Blujepa (gepotidacin) | Triazaacenaphthylene antibiotic (uUTI) | Mkt |
| Depemokimab | Ultra-long-acting anti-IL-5 | Filed/Ph3 |
| Bepirovirsen | HBV antisense (functional cure) | Ph3 |
| Camlipixant | P2X3 antagonist (chronic cough) | Ph3 |
| Linerixibat | IBAT inhibitor (PBC cholestatic pruritus) | Filed/Ph3 |
| Tebipenem HBr | Oral carbapenem (cUTI, w/ Spero) | Filed |
| Dostarlimab perioperative (dMMR) | Anti-PD-1 | Ph3 |
| Nucala COPD | Anti-IL-5 | Mkt |
| Cobolimab | Anti-TIM-3 | Ph3 |
| GSK'227 (nectin-4 ADC) | ADC (bladder) | Ph2 |
| Belrestotug (EOS-448) | Anti-TIGIT (w/ iTeos) | Ph2 |
| Efimosfermin | FGF21 analog (MASH, w/ Boston Pharma) | Ph3 |

## Novartis (NVS)

| Compound | Class | Status |
|----------|-------|--------|
| Entresto (sacubitril/valsartan) | ARNI (heart failure) | Mkt (LOE/generics) |
| Cosentyx (secukinumab) | IL-17A inhibitor | Mkt |
| Kesimpta (ofatumumab) | Anti-CD20 (MS, SC) | Mkt |
| Kisqali (ribociclib) | CDK4/6 inhibitor | Mkt |
| Leqvio (inclisiran) | PCSK9 siRNA | Mkt |
| Pluvicto (177Lu-PSMA-617) | Radioligand therapy (prostate) | Mkt |
| Scemblix (asciminib) | STAMP BCR-ABL inhibitor | Mkt |
| Tafinlar (dabrafenib) | BRAF inhibitor | Mkt |
| Mekinist (trametinib) | MEK inhibitor | Mkt |
| Jakavi (ruxolitinib) | JAK1/2 (ex-US, w/ Incyte) | Mkt |
| Promacta / Revolade (eltrombopag) | TPO receptor agonist | Mkt |
| Tasigna (nilotinib) | BCR-ABL TKI | Mkt/LOE |
| Zolgensma (onasemnogene) | SMN1 gene therapy (SMA) | Mkt |
| Fabhalta (iptacopan) | Factor B inhibitor (PNH, IgAN, C3G) | Mkt |
| Lutathera (177Lu-DOTATATE) | Radioligand (NETs) | Mkt |
| Xolair (omalizumab) | Anti-IgE (w/ Roche) | Mkt |
| Ilaris (canakinumab) | Anti-IL-1β | Mkt |
| Vijoice (alpelisib) | PI3Kα inhibitor (PROS) | Mkt |
| Sandostatin (octreotide) | Somatostatin analog | Mkt/LOE |
| Remibrutinib | Covalent BTK inhibitor (CSU, MS) | Filed/Ph3 |
| Ianalumab | Anti-BAFF-R (Sjögren's, ITP, SLE) | Ph3 |
| Atrasentan | Endothelin A antagonist (IgA nephropathy) | Filed/Ph3 |
| Pelabresib | BET inhibitor (myelofibrosis) | Filed/Ph3 |
| Zigakibart (anti-APRIL) | IgA nephropathy | Ph3 |
| Ligelizumab | Anti-IgE (CSU/food allergy) | Ph3 |
| Leqvio primary prevention | PCSK9 siRNA | Ph3 |
| Pluvicto pre-taxane / earlier lines | Radioligand | Mkt |
| 225Ac next-gen radioligands | Alpha radioligand | Ph1/2 |
| OAV101 IT (Zolgensma intrathecal) | SMN1 gene therapy | Filed |
| Ribociclib adjuvant (early breast) | CDK4/6 | Mkt |
| YTB323 (rapcabtagene) | CD19 CAR-T (autoimmune) | Ph2 |

## Novo Nordisk (NVO)

| Compound | Class | Status |
|----------|-------|--------|
| Ozempic (semaglutide) | GLP-1 agonist (T2D) | Mkt |
| Wegovy (semaglutide 2.4mg) | GLP-1 agonist (obesity/CV) | Mkt |
| Rybelsus (oral semaglutide) | Oral GLP-1 agonist | Mkt |
| Victoza (liraglutide) | GLP-1 agonist | Mkt/LOE |
| Saxenda (liraglutide 3mg) | GLP-1 agonist (obesity) | Mkt/LOE |
| Tresiba (insulin degludec) | Ultra-long basal insulin | Mkt |
| NovoRapid/NovoLog (insulin aspart) | Rapid-acting insulin | Mkt |
| Fiasp (faster aspart) | Ultra-rapid insulin | Mkt |
| Awiqli (insulin icodec) | Once-weekly basal insulin | Mkt (ex-US)/Filed |
| Sogroya (somapacitan) | Long-acting growth hormone | Mkt |
| Norditropin | Growth hormone | Mkt |
| Alhemo (concizumab) | Anti-TFPI (hemophilia) | Mkt |
| Rivfloza (nedosiran) | Hepatic LDH siRNA (PH1) | Mkt |
| Levemir (insulin detemir) | Basal insulin | Disc (US) |
| CagriSema (cagrilintide + semaglutide) | Amylin + GLP-1 (obesity) | Filed/Ph3 |
| Cagrilintide | Long-acting amylin analog | Ph3 |
| Amycretin | Oral/SC amylin + GLP-1 (obesity) | Ph2 |
| Semaglutide MASH | GLP-1 | Filed/Ph3 |
| Semaglutide Alzheimer's (EVOKE) | GLP-1 | Ph3 |
| Oral semaglutide 25mg obesity | Oral GLP-1 | Filed |
| Monlunabant | CB1 inverse agonist (obesity) | Ph2 |
| Ziltivekimab | Anti-IL-6 (CV inflammation, HFpEF) | Ph3 |
| Mim8 (denecimig) | Factor VIIIa mimetic bispecific (hemophilia A) | Ph3 |
| Etavopivat | PKR activator (sickle cell) | Ph3 |
| Semaglutide PAD / knee OA | GLP-1 | Ph3 |

## Roche / Genentech (RHHBY)

| Compound | Class | Status |
|----------|-------|--------|
| Ocrevus (ocrelizumab) | Anti-CD20 (MS) | Mkt |
| Ocrevus Zunovo (SC) | Anti-CD20 SC | Mkt |
| Hemlibra (emicizumab) | Factor IXa/X bispecific (hemophilia A) | Mkt |
| Perjeta (pertuzumab) | Anti-HER2 mAb | Mkt |
| Phesgo (pertuzumab/trastuzumab SC) | HER2 combo SC | Mkt |
| Herceptin (trastuzumab) | Anti-HER2 mAb | Mkt/LOE |
| Avastin (bevacizumab) | Anti-VEGF mAb | Mkt/LOE |
| Rituxan / MabThera (rituximab) | Anti-CD20 mAb | Mkt/LOE |
| Tecentriq (atezolizumab) | Anti-PD-L1 mAb | Mkt |
| Tecentriq Hybreza (SC) | Anti-PD-L1 SC | Mkt |
| Actemra / RoActemra (tocilizumab) | Anti-IL-6R | Mkt/LOE |
| Vabysmo (faricimab) | Anti-VEGF-A/Ang-2 bispecific (eye) | Mkt |
| Evrysdi (risdiplam) | SMN2 splicing modifier (SMA, oral) | Mkt |
| Xolair (omalizumab) | Anti-IgE (w/ Novartis) | Mkt |
| Kadcyla (T-DM1) | HER2 ADC | Mkt |
| Polivy (polatuzumab vedotin) | CD79b ADC | Mkt |
| Lunsumio (mosunetuzumab) | CD20×CD3 bispecific | Mkt |
| Columvi (glofitamab) | CD20×CD3 bispecific | Mkt |
| Alecensa (alectinib) | ALK inhibitor | Mkt |
| Gazyva/Gazyvaro (obinutuzumab) | Anti-CD20 (glycoengineered) | Mkt |
| Esbriet (pirfenidone) | Antifibrotic (IPF) | Mkt/LOE |
| Xofluza (baloxavir) | Cap-dependent endonuclease inhibitor (flu) | Mkt |
| Susvimo (ranibizumab implant) | Anti-VEGF port delivery | Mkt |
| Piasky (crovalimab) | Anti-C5 (PNH) | Mkt |
| Itovebi (inavolisib) | PI3Kα inhibitor (breast) | Mkt |
| Gazyva lupus nephritis / IgAN | Anti-CD20 | Filed/Ph3 |
| Astegolimab | Anti-ST2/IL-33R (COPD) | Ph3 |
| Trontinemab | Brain-shuttle anti-amyloid (Alzheimer's) | Ph3 |
| Prasinezumab | Anti-alpha-synuclein (Parkinson's, w/ Prothena) | Ph2 |
| Fenebrutinib | Non-covalent BTK inhibitor (MS) | Ph3 |
| Giredestrant | Oral SERD (breast) | Ph3 |
| Divarasib (GDC-6036) | KRAS G12C inhibitor | Ph2/3 |
| Tiragolumab | Anti-TIGIT | Ph3 |
| Cevostamab | FcRH5×CD3 bispecific (myeloma) | Ph2 |
| Inavolisib expansions | PI3Kα | Ph3 |
| Faricimab high-dose / RVO | Anti-VEGF/Ang-2 | Ph3 |
| NXT007 (next-gen hemophilia) | Factor VIIIa mimetic | Ph1/2 |

---

## Biogen (BIIB)

| Compound | Class | Status |
|----------|-------|--------|
| Leqembi (lecanemab) | Anti-amyloid protofibril (Alzheimer's, w/ Eisai) | Mkt |
| Spinraza (nusinersen) | SMN2 antisense (SMA) | Mkt |
| Tysabri (natalizumab) | Anti-α4-integrin (MS) | Mkt/LOE |
| Vumerity (diroximel fumarate) | Nrf2 activator (MS) | Mkt |
| Tecfidera (dimethyl fumarate) | Nrf2 activator (MS) | Mkt/LOE |
| Skyclarys (omaveloxolone) | Nrf2 activator (Friedreich's ataxia) | Mkt |
| Qalsody (tofersen) | SOD1 antisense (ALS) | Mkt |
| Zurzuvae (zuranolone) | Neuroactive steroid (postpartum depression) | Mkt |
| Plegridy (peginterferon β-1a) | Interferon (MS) | Mkt |
| Fampyra (dalfampridine) | K+ channel blocker (MS walking) | Mkt |
| Benepali / Imraldi / Flixabi | Anti-TNF biosimilars (EU) | Mkt/Bx |
| Tofidence (tocilizumab) | Anti-IL-6R biosimilar | Mkt/Bx |
| Aduhelm (aducanumab) | Anti-amyloid mAb | Disc (2024) |
| Lecanemab SC maintenance | Anti-amyloid | Filed/Mkt |
| Salanersen (BIIB115) | Next-gen SMN2 antisense | Ph1 |
| Dapirolizumab pegol | Anti-CD40L (SLE, w/ UCB) | Filed/Ph3 |
| Litifilimab (BIIB059) | Anti-BDCA2 (lupus, CLE) | Ph3 |
| Felzartamab | Anti-CD38 (renal/autoimmune, w/ HI-Bio) | Ph2/3 |
| BIIB080 | Anti-tau antisense (Alzheimer's, w/ Ionis) | Ph2 |
| Zorevunersen | Antisense (Dravet syndrome, w/ Stoke) | Ph3 |
| Tofersen presymptomatic (ATLAS) | SOD1 antisense | Ph3 |

## Regeneron (REGN)

| Compound | Class | Status |
|----------|-------|--------|
| Eylea (aflibercept) | Anti-VEGF trap (eye) | Mkt/LOE |
| Eylea HD (aflibercept 8mg) | High-dose anti-VEGF | Mkt |
| Dupixent (dupilumab) | Anti-IL-4Rα (w/ Sanofi) | Mkt |
| Libtayo (cemiplimab) | Anti-PD-1 mAb | Mkt |
| Praluent (alirocumab) | Anti-PCSK9 mAb | Mkt |
| Kevzara (sarilumab) | Anti-IL-6R (w/ Sanofi) | Mkt |
| Inmazeb (REGN-EB3) | Anti-Ebola mAb cocktail | Mkt |
| Evkeeza (evinacumab) | Anti-ANGPTL3 (HoFH) | Mkt |
| Veopoz (pozelimab) | Anti-C5 (CHAPLE disease) | Mkt |
| Ordspono (odronextamab) | CD20×CD3 bispecific | Mkt (EU) |
| Lynozyfic (linvoseltamab) | BCMA×CD3 bispecific (myeloma) | Mkt |
| Dupixent COPD / CSU / bullous pemphigoid | Anti-IL-4Rα | Mkt |
| Itepekimab | Anti-IL-33 (COPD, w/ Sanofi) | Ph3 (mixed) |
| Fianlimab | Anti-LAG-3 (+ cemiplimab, melanoma) | Ph3 |
| Garetosmab | Anti-activin A (FOP) | Ph3 |
| Cemiplimab adjuvant CSCC | Anti-PD-1 | Ph3 |
| Odronextamab earlier lines | CD20×CD3 | Ph3 |
| REGN5678 (anti-PSMAxCD28) | Costimulatory bispecific (prostate) | Ph1/2 |
| Pozelimab + cemdisiran | C5 combo (myasthenia/PNH) | Ph3 |
| Dupilumab pediatric / new atopy | Anti-IL-4Rα | Ph3 |

## Moderna (MRNA)

| Compound | Class | Status |
|----------|-------|--------|
| Spikevax (elasomeran) | COVID-19 mRNA vaccine | Mkt |
| mNEXSPIKE (mRNA-1283) | Next-gen COVID mRNA vaccine | Mkt |
| mResvia (mRNA-1345) | RSV mRNA vaccine (older adults) | Mkt |
| mRNA-1010 | Seasonal influenza mRNA vaccine | Ph3/Filed |
| mRNA-1045 / mRNA-1230 | Flu+COVID / flu+COVID+RSV combos | Ph3 |
| mRNA-4157 (intismeran autogene / V940) | Individualized neoantigen (w/ Merck) | Ph3 |
| mRNA-1647 | CMV vaccine | Ph3 |
| mRNA-1083 | COVID + flu combo | Filed |
| mRNA-3927 | Propionic acidemia therapeutic | Ph1/2 |
| mRNA-3705 | Methylmalonic acidemia | Ph1/2 |
| mRNA-1195 / mRNA-1189 | EBV vaccines | Ph1 |
| mRNA-1608 | Herpes simplex (HSV-2) vaccine | Ph1/2 |
| mRNA-1403 | Norovirus vaccine | Ph3 |
| mRNA-3745 | Glycogen storage disease Ia | Ph1 |

## BioNTech (BNTX)

| Compound | Class | Status |
|----------|-------|--------|
| Comirnaty (tozinameran) | COVID mRNA vaccine (w/ Pfizer) | Mkt |
| BNT327 (PM8002) | PD-L1×VEGF-A bispecific | Ph3 |
| BNT323 (trastuzumab pamirtecan) | HER2 ADC (w/ DualityBio) | Ph3 |
| BNT122 (autogene cevumeran) | Individualized neoantigen (w/ Genentech) | Ph2 |
| BNT111 | Melanoma FixVac (mRNA) | Ph2 |
| BNT116 | NSCLC mRNA immunotherapy | Ph1/2 |
| BNT113 | HPV16+ head & neck mRNA | Ph2 |
| BNT142 | CLDN6×CD3 mRNA-encoded bispecific | Ph1 |
| BNT165 | Malaria mRNA vaccine | Ph1 |
| BNT163 | Shingles mRNA vaccine (w/ Pfizer) | Ph1 |
| Combo COVID/flu (w/ Pfizer) | mRNA vaccine | Ph3 |

## Incyte (INCY)

| Compound | Class | Status |
|----------|-------|--------|
| Jakafi (ruxolitinib) | JAK1/2 inhibitor | Mkt (LOE ~2028) |
| Opzelura (ruxolitinib cream) | Topical JAK1/2 (vitiligo, AD) | Mkt |
| Pemazyre (pemigatinib) | FGFR inhibitor | Mkt |
| Monjuvi / Minjuvi (tafasitamab) | Anti-CD19 (DLBCL) | Mkt |
| Iclusig (ponatinib) | BCR-ABL TKI | Mkt |
| Zynyz (retifanlimab) | Anti-PD-1 | Mkt |
| Niktimvo (axatilimab) | Anti-CSF1R (chronic GVHD) | Mkt |
| Jakafi XR (once-daily) | JAK1/2 extended-release | Filed |
| Povorcitinib | Oral JAK1 (HS, vitiligo) | Ph3/Filed |
| Tafasitamab (follicular + lenalidomide) | Anti-CD19 | Filed |
| INCB123667 | CDK2 inhibitor | Ph2 |
| Retifanlimab combos | Anti-PD-1 | Ph3 |
| Zilurgisertib (ALK2) | FOP / anemia | Ph2 |

## Exelixis (EXEL)

| Compound | Class | Status |
|----------|-------|--------|
| Cabometyx (cabozantinib) | Multi-kinase (VEGFR/MET/AXL) inhibitor | Mkt |
| Cometriq (cabozantinib) | Multi-kinase (medullary thyroid) | Mkt |
| Cotellic (cobimetinib) | MEK inhibitor (w/ Genentech) | Mkt |
| Cabometyx NET (neuroendocrine) | Multi-kinase | Mkt |
| Zanzalintinib (XL092) | Next-gen VEGFR/MET/AXL inhibitor | Ph3 |
| XL309 (INX-315) | CDK2 inhibitor | Ph1 |
| XB010 | 5T4 ADC | Ph1 |
| XB628 | PD-L1×NKG2A bispecific | Ph1 |
| Zanzalintinib + IO (CRC/RCC/HNSCC) | TKI + checkpoint | Ph3 |

## Jazz Pharmaceuticals (JAZZ)

| Compound | Class | Status |
|----------|-------|--------|
| Xywav (Ca/Mg/K/Na oxybates) | Low-sodium oxybate (narcolepsy) | Mkt |
| Xyrem (sodium oxybate) | Oxybate (narcolepsy) | Mkt/LOE |
| Epidiolex / Epidyolex (cannabidiol) | CBD (epilepsy) | Mkt |
| Rylaze / Enrylaze (asparaginase erwinia) | ALL chemo component | Mkt |
| Zepzelca (lurbinectedin) | Alkylating/transcription inhibitor (SCLC) | Mkt |
| Vyxeos (daunorubicin/cytarabine) | Liposomal chemo (AML) | Mkt |
| Defitelio (defibrotide) | VOD/SOS therapy | Mkt |
| Sunosi (solriamfetol) | DNRI (wakefulness) | Mkt |
| Ziihera (zanidatamab) | HER2 biparatopic bispecific | Mkt |
| Zanidatamab 1L GEA / biliary | HER2 bispecific | Ph3/Filed |
| Zepzelca 1L maintenance (w/ atezolizumab) | Alkylator combo | Filed |
| JZP441 | Orexin-2 agonist (narcolepsy) | Ph1/2 |
| Suvecaltamide (JZP385) | T-type calcium modulator (tremor) | Ph2 |

## Apogee Therapeutics (APGE)

| Compound | Class | Status |
|----------|-------|--------|
| APG777 | Long-acting anti-IL-13 (atopic derm) | Ph2 |
| APG808 | Anti-IL-4Rα (asthma/COPD) | Ph1/2 |
| APG990 | Anti-OX40L (atopic derm) | Ph1 |
| APG279 (APG777 + APG990) | IL-13 + OX40L combo | Ph1 |
| APG333 | Anti-TSLP | Preclinical/Ph1 |

## ImmunityBio (IBRX)

| Compound | Class | Status |
|----------|-------|--------|
| Anktiva (nogapendekin alfa inbakicept / N-803) | IL-15 superagonist (bladder, w/ BCG) | Mkt |
| Anktiva + BCG (NMIBC papillary) | IL-15 superagonist | Filed/Ph3 |
| Anktiva (lung, other solid tumors) | IL-15 superagonist | Ph2 |
| PD-L1 t-haNK | Off-the-shelf NK cell therapy | Ph1/2 |

## Viatris (VTRS)

| Compound | Class | Status |
|----------|-------|--------|
| Lyrica (pregabalin) | α2δ ligand (legacy branded/generic) | Mkt/LOE |
| Lipitor (atorvastatin) | Statin (legacy) | Mkt/LOE |
| Norvasc / Viagra / Zoloft (legacy Upjohn) | Various legacy brands | Mkt/LOE |
| EpiPen (epinephrine autoinjector) | Emergency epinephrine | Mkt |
| Yupelri (revefenacin) | Nebulized LAMA (COPD) | Mkt |
| Tyrvaya (varenicline nasal) | Nicotinic agonist (dry eye) | Mkt |
| Breyna (budesonide/formoterol) | Symbicort generic | Mkt/Bx |
| Semglee (insulin glargine-yfgn) | Interchangeable insulin biosimilar | Mkt/Bx |
| Hulio (adalimumab) | Humira biosimilar | Mkt/Bx |
| Wixela Inhub (fluticasone/salmeterol) | Advair generic | Mkt |
| Ogivri / Fulphila / Nyvepria | Oncology biosimilars (trastuzumab/pegfilgrastim) | Mkt/Bx |
| Effexor / Amitiza / Dymista | Legacy branded | Mkt |
| Meloxicam / complex generics | Pipeline generics | Filed |

## Zoetis (ZTS) — animal health

| Compound | Class | Status |
|----------|-------|--------|
| Apoquel (oclacitinib) | JAK inhibitor (canine dermatitis) | Mkt |
| Cytopoint (lokivetmab) | Anti-IL-31 mAb (canine itch) | Mkt |
| Simparica / Simparica Trio (sarolaner) | Isoxazoline parasiticide | Mkt |
| Librela (bedinvetmab) | Anti-NGF mAb (canine OA pain) | Mkt |
| Solensia (frunevetmab) | Anti-NGF mAb (feline OA pain) | Mkt |
| Revolution / Revolution Plus (selamectin) | Parasiticide | Mkt |
| Convenia (cefovecin) | Long-acting antibiotic | Mkt |
| Draxxin (tulathromycin) | Cattle/swine antibiotic | Mkt |
| Rimadyl (carprofen) | NSAID (canine) | Mkt/LOE |
| ProHeart (moxidectin) | Heartworm prevention | Mkt |
| Vanguard / Versican | Canine vaccines | Mkt |
| Excede (ceftiofur) | Livestock antibiotic | Mkt |
| Improvest | Boar taint immunological | Mkt |
| Bonqat (pregabalin, feline) | Anxiolytic | Mkt |
| Adequan (PSGAG) | Osteoarthritis (equine/canine) | Mkt |
| Librela (equine/expanded) | Anti-NGF | Ph3 |
| Zoetis mAb pipeline (parvovirus, etc.) | Monoclonal antibodies | Ph2/3 |
| Next-gen triple parasiticides | Isoxazoline combos | Ph3 |

---

## Key competitors & partners (external, price-moving)

These are not book tickers but their compounds directly move book names via competition,
partnership, or category read-through.

### Sanofi (SNY) — partner to REGN, competitor across I&I/vaccines

| Compound | Class | Status |
|----------|-------|--------|
| Dupixent (dupilumab) | Anti-IL-4Rα (w/ Regeneron) | Mkt |
| Beyfortus (nirsevimab) | Anti-RSV mAb (w/ AZN) | Mkt |
| Sarclisa (isatuximab) | Anti-CD38 (myeloma) | Mkt |
| Altuviiio (efanesoctocog alfa) | Long-acting Factor VIII | Mkt |
| Qfitlia (fitusiran) | Antithrombin siRNA (hemophilia) | Mkt |
| Nexviazyme (avalglucosidase) | Enzyme replacement (Pompe) | Mkt |
| Lantus / Toujeo (insulin glargine) | Basal insulin | Mkt/LOE |
| Aubagio (teriflunomide) | DHODH inhibitor (MS) | Mkt/LOE |
| Wayrilz (rilzabrutinib) | Reversible covalent BTK (ITP) | Mkt/Filed |
| Tolebrutinib | CNS-penetrant BTK (MS) | Filed/Ph3 |
| Amlitelimab | Anti-OX40L (atopic derm) | Ph3 |
| Frexalimab | Anti-CD40L (MS) | Ph3 |
| Tzield (teplizumab) | Anti-CD3 (T1D delay) | Mkt |

### Vertex (VRTX) — CF franchise + pain/renal expansion

| Compound | Class | Status |
|----------|-------|--------|
| Trikafta / Kaftrio (elexacaftor/tezacaftor/ivacaftor) | CFTR modulator | Mkt |
| Alyftrek (vanza triple) | Next-gen CFTR modulator | Mkt |
| Journavx (suzetrigine, VX-548) | NaV1.8 inhibitor (acute pain) | Mkt |
| Casgevy (exa-cel) | CRISPR gene editing (SCD/β-thal, w/ CRISPR Tx) | Mkt |
| Symdeko / Orkambi / Kalydeco | Earlier CFTR modulators | Mkt |
| Suzetrigine (diabetic neuropathy, LSR) | NaV1.8 inhibitor | Ph3 |
| Inaxaplin (VX-147) | APOL1 inhibitor (kidney disease) | Ph3 |
| Zimislecel (VX-880) | Stem-cell islet therapy (T1D) | Ph1/2/3 |
| Povetacicept | APRIL/BAFF trap (IgAN, w/ Alpine) | Ph3 |

### GLP-1 / obesity challengers (move LLY, NVO, AMGN, PFE)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Survodutide | Boehringer/Zealand | GLP-1/glucagon dual | Ph3 |
| Petrelintide | Zealand/Roche | Long-acting amylin | Ph2/3 |
| VK2735 | Viking Therapeutics | GIP/GLP-1 dual (SC + oral) | Ph3/Ph2 |
| GSBR-1290 | Structure Therapeutics | Oral small-molecule GLP-1 | Ph2 |
| Pemvidutide | Altimmune | GLP-1/glucagon dual (MASH/obesity) | Ph2 |
| MET-097i / MET-233i | Metsera (→ Pfizer) | Ultra-long GLP-1 / amylin | Ph2 |
| CT-388 | Carmot → Roche | GLP-1/GIP | Ph2 |
| Retatrutide | Lilly | Triple agonist | Ph3 |
| CagriSema | Novo Nordisk | Amylin + GLP-1 | Ph3/Filed |
| MariTide | Amgen | GIPR antag/GLP-1 | Ph3 |

### Checkpoint / IO challengers (move MRK Keytruda, BMY, RHHBY, AZN)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Ivonescimab | Summit/Akeso | PD-1×VEGF bispecific | Ph3 |
| Toripalimab (Loqtorzi) | Coherus/Junshi | Anti-PD-1 | Mkt |
| Tislelizumab (Tevimbra) | BeOne (BeiGene) | Anti-PD-1 | Mkt |
| Cosibelimab (Unloxcyt) | Checkpoint Tx | Anti-PD-L1 | Mkt |
| Botensilimab | Agenus | Fc-enhanced anti-CTLA-4 | Ph2 |

### Alzheimer's & neuro (read-through to LLY, BIIB, RHHBY)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Leqembi (lecanemab) | Eisai/Biogen | Anti-amyloid | Mkt |
| Kisunla (donanemab) | Lilly | Anti-amyloid | Mkt |
| Trontinemab | Roche | Brain-shuttle anti-amyloid | Ph3 |
| Remternetug | Lilly | Anti-amyloid (SC) | Ph3 |
| Blarcamesine | Anavex | Sigma-1 agonist | Filed |

### Biosimilar wave (erodes AbbVie, JNJ, AMGN, Roche, Regeneron)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Amjevita / Simlandi / Yuflyma / Hadlima / Hyrimoz / Cyltezo / Idacio | Amgen / Teva-Alvotech / Celltrion / Organon / Sandoz / Boehringer / Fresenius | Adalimumab (Humira) biosimilars | Mkt/Bx |
| Wezlana / Selarsdi / Steqeyma / Otulfi / Pyzchiva / Imuldosa | Amgen / Teva-Alvotech / Celltrion / Fresenius-Formycon / Samsung / Accord | Ustekinumab (Stelara) biosimilars | Mkt/Bx |
| Pavblu / Opuviz / Yesafili / Enzeevu / Ahzantive | Amgen / Samsung / Biocon / Sandoz / Formycon | Aflibercept (Eylea) biosimilars | Mkt/Bx |
| Tyruko | Sandoz | Natalizumab (Tysabri) biosimilar | Mkt/Bx |
| Wyost/Jubbonti, Ospomyv/Xbryk, Conexxence/Bomyntra | Sandoz / Samsung / Fresenius | Denosumab (Prolia/Xgeva) biosimilars | Mkt/Bx |
| Various trastuzumab / bevacizumab / rituximab biosimilars | Amgen / Pfizer / Celltrion / Samsung | Roche legacy biosimilars | Mkt/Bx |

### Other notable price-movers (targeted)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Vyvgart / Vyvgart Hytrulo (efgartigimod) | Argenx | FcRn inhibitor (gMG, CIDP) — competes w/ JNJ nipocalimab | Mkt |
| Rystiggo (rozanolixizumab) | UCB | FcRn inhibitor | Mkt |
| Bimzelx (bimekizumab) | UCB | IL-17A/F inhibitor — competes w/ Cosentyx/Taltz | Mkt |
| Amvuttra / Onpattro (vutrisiran/patisiran) | Alnylam | TTR siRNA — competes w/ PFE tafamidis | Mkt |
| Attruby (acoramidis) | BridgeBio | TTR stabilizer (ATTR-CM) | Mkt |
| Voxzogo (vosoritide) | BioMarin | CNP analog (achondroplasia) | Mkt |
| Roctavian (valoctocogene) | BioMarin | Hemophilia A gene therapy | Mkt |
| Elevidys (delandistrogene) | Sarepta/Roche | DMD gene therapy | Mkt |
| Zolbetuximab (Vyloy) | Astellas | Anti-CLDN18.2 (gastric) | Mkt |
| Ohtuvayre (ensifentrine) | Verona Pharma | PDE3/4 inhibitor (COPD) | Mkt |
| Winrevair — see Merck | Merck | Activin trap (PAH) | Mkt |
| Cobenfy — see BMS | BMS (Karuna) | Muscarinic (schizophrenia) | Mkt |

---

## Extended competitor & catalyst universe

Broader competitor/read-through coverage, grouped by the therapeutic **battlegrounds** where
book prices are actually set. These are external names (mostly mid-cap biotech + big-pharma
partners) whose compounds compete with, validate, or threaten book holdings. Column adds
**Owner**. Same status legend.

### MASH / metabolic-liver (read-through to NVO, LLY, GSK, MRK)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Rezdiffra (resmetirom) | Madrigal | Oral THR-β agonist (first MASH approval) | Mkt |
| Efruxifermin | Akero | FGF21 analog | Ph3 |
| Pegozafermin | 89bio | FGF21 analog | Ph3 |
| Efimosfermin alfa | GSK (Boston Pharma) | FGF21 analog | Ph3 |
| Survodutide (MASH) | Boehringer/Zealand | GLP-1/glucagon dual | Ph3 |
| Semaglutide (MASH) | Novo Nordisk | GLP-1 | Filed/Ph3 |
| Tirzepatide (MASH) | Lilly | GIP/GLP-1 | Ph3 |
| Lanifibranor | Inventiva | Pan-PPAR agonist | Ph3 |
| VK2809 | Viking | THR-β agonist | Ph2 |
| Denifanstat | Sagimet | FASN inhibitor | Ph3 |
| Pemvidutide (MASH) | Altimmune | GLP-1/glucagon | Ph2 |

### Cardiometabolic — Lp(a), lipids, CETP (read-through to AMGN, LLY, NVS, PFE)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Pelacarsen | Novartis (Ionis) | Lp(a) antisense | Ph3 |
| Olpasiran | Amgen | Lp(a) siRNA | Ph3 |
| Lepodisiran | Lilly | Lp(a) siRNA | Ph3 |
| Zerlasiran | Silence Therapeutics | Lp(a) siRNA | Ph2 |
| Muvalaplin | Lilly | Oral Lp(a) inhibitor | Ph2 |
| Obicetrapib | NewAmsterdam | Oral CETP inhibitor (LDL) | Ph3/Filed |
| Nexletol/Nexlizet (bempedoic acid) | Esperion | ACL inhibitor | Mkt |
| VERVE-102 | Verve/Lilly | PCSK9 base-editing (in vivo) | Ph1 |
| Plozasiran | Arrowhead | APOC3 siRNA (hypertriglyceridemia) | Filed/Ph3 |
| Enlicitide — see Merck | Merck | Oral PCSK9 | Ph3 |

### Cardiomyopathy & ATTR (read-through to BMY, PFE, AZN)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Aficamten | Cytokinetics | Cardiac myosin inhibitor (HCM) — vs Camzyos | Filed/Ph3 |
| Camzyos (mavacamten) | BMS | Myosin inhibitor (HCM) | Mkt |
| Attruby (acoramidis) | BridgeBio | TTR stabilizer (ATTR-CM) | Mkt |
| Amvuttra (vutrisiran) | Alnylam | TTR siRNA (ATTR-CM/PN) | Mkt |
| Wainua (eplontersen) | AZN/Ionis | TTR antisense | Mkt |
| Nexiguran ziclumeran (NTLA-2001) | Intellia/Regeneron | In-vivo CRISPR TTR knockout | Ph3 |
| Vyndaqel/Vyndamax — see Pfizer | Pfizer | TTR stabilizer | Mkt |
| Omecamtiv mecarbil | Cytokinetics | Cardiac myosin activator (HFrEF) | Disc/Ph3 |
| Ninerafaxstat | Imbria | Cardiac metabolic modulator | Ph2 |

### Obesity — extended challengers (read-through to LLY, NVO, AMGN)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| VK2735 (oral + SC) | Viking | GIP/GLP-1 dual | Ph3/Ph2 |
| GSBR-1290 | Structure Therapeutics | Oral GLP-1 | Ph2 |
| MET-097i | Metsera/Pfizer | Ultra-long GLP-1 | Ph2 |
| MET-233i | Metsera/Pfizer | Amylin analog | Ph1/2 |
| Petrelintide | Zealand/Roche | Amylin analog | Ph2/3 |
| CT-388 | Roche (Carmot) | GLP-1/GIP | Ph2 |
| CT-996 | Roche (Carmot) | Oral GLP-1 | Ph1 |
| Survodutide (obesity) | Boehringer/Zealand | GLP-1/glucagon | Ph3 |
| TERN-601 | Terns | Oral GLP-1 | Ph2 |
| HU6 | Rivus | Controlled metabolic accelerator | Ph2 |
| Amycretin | Novo Nordisk | Amylin/GLP-1 | Ph2 |
| Bimagrumab | Lilly (Versanis) | ActRII mAb (muscle preservation) | Ph2 |
| Ecnoglutide | (Sciwind) | GLP-1 | Ph3 (China) |
| Mazdutide | Innovent/Lilly | GLP-1/glucagon | Ph3 (China) |

### Autoimmune — FcRn & B-cell (read-through to JNJ, AZN, BIIB, AMGN)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Vyvgart / Vyvgart Hytrulo (efgartigimod) | Argenx | FcRn inhibitor | Mkt |
| Rystiggo (rozanolixizumab) | UCB | FcRn inhibitor | Mkt |
| Imaavy (nipocalimab) | JNJ | FcRn inhibitor | Mkt |
| Batoclimab | Immunovant/Roivant | FcRn inhibitor | Ph3 |
| IMVT-1402 | Immunovant/Roivant | Next-gen FcRn inhibitor | Ph2/3 |
| Empasiprubart | Argenx | Anti-C2 (CIDP, MMN) | Ph2 |
| Uplizna (inebilizumab) | Amgen | Anti-CD19 | Mkt |
| Rituximab / obinutuzumab (autoimmune) | Roche | Anti-CD20 | Mkt |
| Frexalimab | Sanofi | Anti-CD40L (MS) | Ph3 |
| Dapirolizumab pegol | UCB/Biogen | Anti-CD40L (SLE) | Ph3 |

### Nephrology — IgAN & complement (read-through to NVS, RHHBY, AZN, REGN)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Fabhalta (iptacopan) | Novartis | Factor B inhibitor | Mkt |
| Vanrafia (atrasentan) | Novartis | Endothelin-A antagonist (IgAN) | Mkt/Filed |
| Filspari (sparsentan) | Travere | Dual ET/AT1 antagonist (IgAN, FSGS) | Mkt |
| Tarpeyo (budesonide) | Calliditas/Asahi | Targeted-release budesonide (IgAN) | Mkt |
| Sibeprenlimab | Otsuka | Anti-APRIL (IgAN) | Ph3 |
| Zigakibart | Novartis | Anti-APRIL (IgAN) | Ph3 |
| Atacicept | Vera Therapeutics | APRIL/BAFF inhibitor (IgAN) | Ph3 |
| Povetacicept | Vertex/Alpine | APRIL/BAFF trap (IgAN) | Ph3 |
| Cemdisiran | Alnylam/Regeneron | C5 siRNA | Ph3 |
| Felzartamab | Biogen/HI-Bio | Anti-CD38 (renal autoimmune) | Ph2/3 |
| Piasky (crovalimab) | Roche | Anti-C5 (PNH) | Mkt |
| Voydeya (danicopan) | AstraZeneca | Factor D inhibitor | Mkt |

### Immunology & derm — extended (read-through to ABBV, JNJ, LLY, NVS, REGN)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Bimzelx (bimekizumab) | UCB | IL-17A/F inhibitor | Mkt |
| Spevigo (spesolimab) | Boehringer | Anti-IL-36R (GPP) | Mkt |
| Sonelokimab | MoonLake | Nanobody IL-17A/F (HS) | Ph3 |
| Tulisokibart (MK-7240) | Merck (Prometheus) | Anti-TL1A (IBD) | Ph3 |
| Duvakitug | Sanofi/Teva | Anti-TL1A (IBD) | Ph2/3 |
| Amlitelimab | Sanofi | Anti-OX40L (atopic derm) | Ph3 |
| Rocatinlimab | Amgen | Anti-OX40 (atopic derm) | Ph3 |
| Lebrikizumab (Ebglyss) | Lilly | Anti-IL-13 | Mkt |
| Nemolizumab (Nemluvio) | Galderma | Anti-IL-31RA (prurigo, AD) | Mkt |
| Deucravacitinib (Sotyktu) | BMS | TYK2 (psoriasis, expanding) | Mkt |
| Zasocitinib (TAK-279) | Takeda | Oral TYK2 | Ph3 |
| Icotrokinra (JNJ-2113) | JNJ | Oral IL-23R peptide | Ph3/Filed |
| Anti-IL-2 (rezpegaldesleukin) | Nektar | Treg-selective IL-2 (atopic derm/AA) | Ph2 |

### Respiratory — extended (read-through to AZN, GSK, REGN, AMGN)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Ohtuvayre (ensifentrine) | Verona | PDE3/4 inhibitor (COPD) | Mkt |
| Dupixent (COPD) | Regeneron/Sanofi | Anti-IL-4Rα | Mkt |
| Tezspire (tezepelumab) | Amgen/AZN | Anti-TSLP | Mkt |
| Depemokimab | GSK | Ultra-long anti-IL-5 | Filed |
| Tozorakimab | AstraZeneca | Anti-IL-33 (COPD) | Ph3 |
| Itepekimab | Regeneron/Sanofi | Anti-IL-33 (COPD) | Ph3 |
| Astegolimab | Roche | Anti-ST2/IL-33R (COPD) | Ph3 |
| Brensocatib (Brinsupri) | Insmed | DPP1 inhibitor (bronchiectasis) | Mkt |
| Camlipixant | GSK | P2X3 antagonist (chronic cough) | Ph3 |

### Oncology — KRAS / RAS (read-through to AMGN, BMY, RHHBY, LLY, MRK)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Lumakras (sotorasib) | Amgen | KRAS G12C | Mkt |
| Krazati (adagrasib) | BMS (Mirati) | KRAS G12C | Mkt |
| Divarasib | Roche | KRAS G12C | Ph2/3 |
| Olomorasib | Lilly | KRAS G12C | Ph2 |
| Daraxonrasib (RMC-6236) | Revolution Medicines | RAS(ON) multi-selective | Ph3 |
| Zoldonrasib (RMC-9805) | Revolution Medicines | KRAS G12D(ON) | Ph1/2 |
| Elironrasib (RMC-6291) | Revolution Medicines | KRAS G12C(ON) | Ph1/2 |
| MK-1084 | Merck | KRAS G12C | Ph2 |
| Glecirasib | Jacobio | KRAS G12C | Ph2 (China) |
| BBP-398 | BridgeBio | SHP2 inhibitor | Ph2 |

### Oncology — ADCs, extended (read-through to AZN, GILD, PFE, ABBV, MRK, JAZZ)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Enhertu (trastuzumab deruxtecan) | Daiichi/AZN | HER2 ADC | Mkt |
| Datroway (datopotamab deruxtecan) | Daiichi/AZN | TROP2 ADC | Mkt |
| Patritumab deruxtecan (HER3-DXd) | Daiichi | HER3 ADC (NSCLC) | Filed/Ph3 |
| Ifinatamab deruxtecan (I-DXd) | Daiichi/Merck | B7-H3 ADC (SCLC) | Ph3 |
| Raludotatug deruxtecan (R-DXd) | Daiichi/Merck | CDH6 ADC (ovarian) | Ph3 |
| Sacituzumab tirumotecan (sac-TMT) | Merck/Kelun | TROP2 ADC | Ph3 |
| Trodelvy (sacituzumab govitecan) | Gilead | TROP2 ADC | Mkt |
| Padcev (enfortumab vedotin) | Pfizer/Astellas | Nectin-4 ADC | Mkt |
| Elahere (mirvetuximab soravtansine) | AbbVie | FRα ADC | Mkt |
| Ziihera (zanidatamab) | Jazz | HER2 bispecific | Mkt |
| Disitamab vedotin | RemeGen/Seagen | HER2 ADC | Mkt (China) |
| Emiltatug ledadotin — n/a | Mersana | B7-H4 ADC | Ph1 |
| ARX788 | Ambrx | HER2 ADC | Ph2/3 |

### Oncology — cell therapy (read-through to JNJ, GILD, BMY)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Carvykti (cilta-cel) | JNJ/Legend | BCMA CAR-T | Mkt |
| Abecma (ide-cel) | BMS/2seventy | BCMA CAR-T | Mkt |
| Yescarta (axi-cel) | Gilead | CD19 CAR-T | Mkt |
| Breyanzi (liso-cel) | BMS | CD19 CAR-T | Mkt |
| Anito-cel (anitocabtagene) | Gilead/Arcellx | BCMA CAR-T | Filed/Ph3 |
| Aucatzyl (obe-cel) | Autolus | CD19 CAR-T (adult ALL) | Mkt |
| CABA-201 | Cabaletta | CD19 CAR-T (autoimmune) | Ph2 |
| KYV-101 | Kyverna | CD19 CAR-T (autoimmune) | Ph2 |
| Rapcabtagene (YTB323) | Novartis | CD19 CAR-T (autoimmune) | Ph2 |
| Tecelra (afami-cel) | Adaptimmune | MAGE-A4 TCR-T (sarcoma) | Mkt |

### Radiopharmaceuticals (read-through to NVS, LLY, BMY, AZN)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Pluvicto (177Lu-PSMA-617) | Novartis | Beta radioligand (prostate) | Mkt |
| Lutathera (177Lu-DOTATATE) | Novartis | Beta radioligand (NET) | Mkt |
| PNT2002 (177Lu-PSMA-I&T) | Lilly (Point Bio) | PSMA radioligand | Filed/Ph3 |
| RYZ101 (225Ac-DOTATATE) | BMS (RayzeBio) | Alpha radioligand (NET) | Ph3 |
| FPI-2265 (225Ac-PSMA) | AstraZeneca (Fusion) | Alpha radioligand (prostate) | Ph2 |
| Pluvicto next-gen (225Ac-PSMA) | Novartis | Alpha radioligand | Ph1/2 |
| TLX591 (177Lu rosopatamab) | Telix | PSMA radioligand | Ph3 |
| Pylarify (18F-piflufolastat) | Lantheus | PSMA PET imaging | Mkt |

### Vaccines — competitor landscape (read-through to PFE, MRNA, GSK, BNTX)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Nuvaxovid | Novavax/Sanofi | Protein-subunit COVID vaccine | Mkt |
| Abrysvo (RSV) | Pfizer | RSV vaccine | Mkt |
| Arexvy (RSV) | GSK | RSV vaccine | Mkt |
| mResvia (RSV) | Moderna | mRNA RSV vaccine | Mkt |
| Penbraya / Prevnar | Pfizer | Meningococcal / pneumococcal | Mkt |
| Capvaxive | Merck | 21-valent pneumococcal | Mkt |
| Heplisav-B | Dynavax | Adjuvanted HBV vaccine | Mkt |
| Jynneos / MVA-BN | Bavarian Nordic | Smallpox/mpox vaccine | Mkt |
| Ixchiq | Valneva | Chikungunya vaccine | Mkt |
| Vimkunya | Bavarian Nordic | Chikungunya vaccine | Mkt |
| mRNA-1010 (flu) | Moderna | mRNA influenza vaccine | Ph3 |

### Rare disease & genetic medicine (read-through to BIIB, RHHBY, NVS, PFE)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Casgevy (exa-cel) | Vertex/CRISPR | Ex-vivo CRISPR (SCD/β-thal) | Mkt |
| Elevidys (delandistrogene) | Sarepta/Roche | DMD gene therapy | Mkt |
| Exondys 51 / Vyondys 53 / Amondys 45 | Sarepta | DMD exon-skipping antisense | Mkt |
| Zolgensma (onasemnogene) | Novartis | SMA gene therapy | Mkt |
| Spinraza (nusinersen) | Biogen/Ionis | SMA antisense | Mkt |
| Evrysdi (risdiplam) | Roche/PTC | SMA splicing modifier | Mkt |
| Tryngolza (olezarsen) | Ionis | APOC3 antisense (FCS) | Mkt |
| Donidalorsen (Dawnzera) | Ionis | Prekallikrein antisense (HAE) | Mkt/Filed |
| Qalsody (tofersen) | Biogen/Ionis | SOD1 antisense (ALS) | Mkt |
| Crysvita (burosumab) | Ultragenyx/Kyowa | Anti-FGF23 (XLH) | Mkt |
| Dojolvi (triheptanoin) | Ultragenyx | LC-FAOD medical food | Mkt |
| Setrusumab | Ultragenyx/Ionis | Anti-sclerostin (OI) | Ph3 |
| Sephience (sepiapterin) | PTC | PKU therapy | Mkt/Filed |
| Voxzogo (vosoritide) | BioMarin | CNP analog (achondroplasia) | Mkt |
| Roctavian (valoctocogene) | BioMarin | Hemophilia A gene therapy | Mkt |
| Hemgenix (etranacogene) | CSL/uniQure | Hemophilia B gene therapy | Mkt |
| Lenmeldy (atidarsagene) | Orchard | MLD gene therapy | Mkt |
| Beqvez (fidanacogene) | Pfizer | Hemophilia B gene therapy | Mkt |
| NTLA-2002 (lonvoguran ziclumeran) | Intellia | In-vivo CRISPR (HAE) | Ph3 |
| Zilebesiran | Alnylam/Roche | AGT siRNA (hypertension) | Ph2 |
| Fitusiran (Qfitlia) | Sanofi/Alnylam | Antithrombin siRNA | Mkt |

### Neuroscience — extended (read-through to BMY, ABBV, JNJ, BIIB, LLY)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Cobenfy (xanomeline/trospium) | BMS (Karuna) | M1/M4 muscarinic | Mkt |
| Caplyta (lumateperone) | JNJ (Intra-Cellular) | Atypical antipsychotic (MDD adjunct) | Mkt |
| Ingrezza (valbenazine) | Neurocrine | VMAT2 inhibitor (TD) | Mkt |
| Austedo (deutetrabenazine) | Teva | VMAT2 inhibitor | Mkt |
| Nuplazid (pimavanserin) | Acadia | 5-HT2A inverse agonist (PDP) | Mkt |
| Auvelity (dextromethorphan/bupropion) | Axsome | NMDA antagonist (MDD) | Mkt |
| Emraclidine | AbbVie (Cerevel) | M4 agonist (schizophrenia) | Disc |
| Tavapadon | AbbVie (Cerevel) | D1/D5 partial agonist (Parkinson's) | Filed |
| Zurzuvae (zuranolone) | Biogen/Sage | Neuroactive steroid (PPD) | Mkt |
| Ulotaront | Sumitomo/Otsuka | TAAR1 agonist (schizophrenia) | Ph3 (mixed) |
| Troriluzole | Biohaven | Glutamate modulator (SCA) | Filed |
| Blarcamesine | Anavex | Sigma-1 agonist (Alzheimer's) | Filed |
| Tividenofusp alfa (DNL310) | Denali | Enzyme transport vehicle (Hunter) | Filed/Ph3 |
| Bexicaserin | Longboard/Lundbeck | 5-HT2C agonist (DEE) | Ph3 |
| NBI-'568 (osavampator?) | Neurocrine | M4 agonist (schizophrenia) | Ph3 |

### Migraine & pain (read-through to PFE, ABBV, LLY, AMGN, VRTX)

| Compound | Owner | Class | Status |
|----------|-------|-------|--------|
| Nurtec ODT (rimegepant) | Pfizer | CGRP antagonist | Mkt |
| Ubrelvy (ubrogepant) | AbbVie | CGRP antagonist | Mkt |
| Qulipta (atogepant) | AbbVie | CGRP antagonist (prevention) | Mkt |
| Emgality (galcanezumab) | Lilly | Anti-CGRP mAb | Mkt |
| Aimovig (erenumab) | Amgen | Anti-CGRP-R mAb | Mkt |
| Ajovy (fremanezumab) | Teva | Anti-CGRP mAb | Mkt |
| Vyepti (eptinezumab) | Lundbeck | Anti-CGRP mAb (IV) | Mkt |
| Journavx (suzetrigine) | Vertex | NaV1.8 inhibitor (acute pain) | Mkt |
| Suzetrigine (DPN/radiculopathy) | Vertex | NaV1.8 inhibitor | Ph3 |
| VX-993 | Vertex | Next-gen NaV1.8 inhibitor | Ph2 |

---

## Appendix A — cross-cutting "battleground" categories

Where price relevance across book names actually concentrates. Use these as thematic baskets.

- **GLP-1 / incretin obesity–diabetes:** tirzepatide (LLY), semaglutide/CagriSema (NVO),
  orforglipron & retatrutide (LLY), MariTide (AMGN), survodutide (BI/Zealand), VK2735 (Viking),
  petrelintide (Zealand/Roche), oral GLP-1s. **The single largest cross-book value driver
  (LLY vs NVO duopoly, with AMGN/PFE/challengers optionality).**
- **PD-1/PD-L1 checkpoint:** Keytruda (MRK, ~40% of category), Opdivo (BMY), Tecentriq (RHHBY),
  Imfinzi (AZN), Libtayo (REGN); next-wave bispecifics ivonescimab & BNT327 threaten Keytruda's
  post-2028 LOE. **Keytruda LOE is the defining MRK catalyst.**
- **Alzheimer's anti-amyloid:** Leqembi (BIIB/Eisai) vs Kisunla (LLY); next-gen trontinemab
  (RHHBY), remternetug (LLY). Category-validation read-through across all three.
- **HER2/TROP2 ADCs:** Enhertu & Datroway (AZN/Daiichi), Trodelvy (GILD), sac-TMT (MRK/Kelun),
  Ziihera (JAZZ). ADC platform value migrating across oncology.
- **Anti-VEGF retina:** Eylea/Eylea HD (REGN) vs Vabysmo (RHHBY) vs aflibercept biosimilars
  (AMGN et al.). **REGN's core cash flow under biosimilar + Vabysmo pressure.**
- **Factor XI/XIa anticoagulants:** milvexian (BMY/JNJ), abelacimab (Anthos), asundexian
  (Bayer, Ph3 setback). Successor category to the Eliquis/Xarelto Factor Xa cliff.
- **Complement (C5 / Factor B / Factor D):** Ultomiris/Soliris (AZN), Fabhalta (NVS),
  Piasky (RHHBY), Voydeya (AZN), pozelimab (REGN).
- **IL-23 / TYK2 / JAK immunology:** Skyrizi & Rinvoq (ABBV, post-Humira engine), Tremfya (JNJ),
  Sotyktu (BMY), Omvoh (LLY), Cosentyx/Bimzelx (NVS/UCB). **ABBV's Humira-cliff replacement thesis.**
- **Respiratory biologics (IL-5/TSLP/IL-33):** Dupixent (REGN/SNY), Tezspire (AMGN/AZN),
  Nucala/depemokimab (GSK), Fasenra (AZN), itepekimab (REGN/SNY), tozorakimab (AZN).
- **Radioligand therapy:** Pluvicto & Lutathera (NVS) — platform with next-gen actinium assets.
- **Patent cliffs (2025–2030) shaping valuations:** Humira (ABBV, live), Stelara (JNJ, live),
  Eylea (REGN), Keytruda (MRK ~2028), Opdivo (BMY ~2028), Eliquis (BMY/PFE ~2028),
  Ibrance (PFE), Jakafi (INCY ~2028), Xtandi, Entresto (NVS), Trulicity/GLP-1 formulation.

## Appendix B — non-compound tickers in the book

These are held names in the pharma/health/life-science complex that **do not make drug
compounds**, so they carry no rows above. Their price relevance is as picks-and-shovels
(tools, devices, distribution) to the compound makers.

| Ticker | Business | Relevance |
|--------|----------|-----------|
| TMO | Thermo Fisher — lab instruments/CDMO | Bioprocessing & pharma services demand |
| DHR | Danaher — life-science tools/diagnostics | Bioprocessing (Cytiva), demand proxy |
| A | Agilent — analytical instruments | Pharma QC/analytics |
| ILMN | Illumina — DNA sequencing | Genomics / dx pipeline enabler |
| TWST | Twist Bioscience — synthetic DNA | Drug discovery inputs |
| RGEN | Repligen — bioprocessing consumables | Biologics manufacturing proxy |
| CRL | Charles River — preclinical CRO | Early pipeline services demand |
| MDT | Medtronic — medical devices | Device, not compound |
| ABT | Abbott — devices/diagnostics/nutrition | Libre CGM, dx |
| BDX | Becton Dickinson — medical devices | Injection/lab hardware |
| DXCM | Dexcom — CGM devices | Diabetes device (GLP-1 read-through) |
| ALC | Alcon — eye-care devices/surgical | Ophthalmology devices |
| GEHC | GE HealthCare — imaging/contrast agents | Imaging (some contrast "agents") |
| CVS | CVS Health — pharmacy/PBM/insurer | Drug distribution & formulary power |
| RPRX | Royalty Pharma — drug royalty aggregator | Owns royalties on many compounds above |

---

*Compiled as a Markdown reference only (not written to the database). Marketed rows are
high-confidence; pipeline names/phases/status are approximate and time-sensitive — verify
against Drugs@FDA, ClinicalTrials.gov, and company pipeline pages before any use that informs
trading.*

***Count:*** *~810 compound rows (~700 unique — a subset of high-impact competitor compounds
is intentionally cross-listed in both its owner's table and the relevant battleground section).
The mix is marketed products + material pipeline + selected patent-cliff/biosimilar entries per
the agreed scope. This is deliberately just under the ~1000 target: the tail was capped rather
than padded with low-materiality legacy generics and obscure early-phase assets, where naming/
status error risk rises. Say the word to extend to a hard 1000 by fleshing out the big-pharma
established-products tails, the Viatris/Zoetis portfolios, and more Phase 1/2 pipeline.*

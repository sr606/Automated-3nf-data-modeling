# System Workflow Visualization

## LangGraph Workflow Diagram (Mermaid)

```mermaid
graph TD
    Start([START]) --> LoadFiles[1. load_files_node<br/>Load CSV/JSON Files]
    LoadFiles --> ExtractMeta[2. extract_metadata_node<br/>Extract Metadata]
    ExtractMeta --> Profile[3. profile_node<br/>Profile Dependencies]
    Profile --> DetectPK[4. detect_primary_keys_node<br/>Detect Primary Keys]
    DetectPK --> DetectFK[5. detect_foreign_keys_node<br/>Detect Foreign Keys]
    DetectFK --> Normalize[6. normalize_3nf_node<br/>Normalize to 3NF]
    Normalize --> GenSQL[7. generate_sql_node<br/>Generate SQL DDL]
    GenSQL --> ValidateSQL[8. validate_sql_node<br/>Validate SQL]
    ValidateSQL --> Export[9. export_outputs_node<br/>Export ERD & Outputs]
    Export --> End([END])
    
    style Start fill:#90EE90
    style End fill:#FFB6C1
    style LoadFiles fill:#87CEEB
    style ExtractMeta fill:#87CEEB
    style Profile fill:#DDA0DD
    style DetectPK fill:#DDA0DD
    style DetectFK fill:#DDA0DD
    style Normalize fill:#FFD700
    style GenSQL fill:#FFA500
    style ValidateSQL fill:#FFA500
    style Export fill:#98FB98
```

## Data Flow Diagram

```mermaid
graph LR
    Input[📁 input_files/<br/>CSV & JSON] --> Meta[📊 Metadata<br/>Extractor]
    Meta --> Prof[🔍 Profiler<br/>Dependencies]
    Prof --> Keys[🔑 Key<br/>Detector]
    Keys --> Norm[⚙️ Normalizer<br/>3NF Engine]
    Norm --> SQL[📝 SQL<br/>Generator]
    SQL --> Out1[📄 normalized_output/]
    SQL --> Out2[💾 sql_output/]
    SQL --> Out3[🎨 erd/]
    
    style Input fill:#E6F3FF
    style Meta fill:#FFE6E6
    style Prof fill:#E6FFE6
    style Keys fill:#FFF4E6
    style Norm fill:#F0E6FF
    style SQL fill:#FFE6F0
    style Out1 fill:#90EE90
    style Out2 fill:#90EE90
    style Out3 fill:#90EE90
```

## Normalization Process

```mermaid
graph TD
    Raw[Raw Data<br/>Unnormalized] --> Check1NF{Check 1NF<br/>Atomic Values?}
    Check1NF -->|No| Fix1NF[Split Multivalued<br/>Columns]
    Check1NF -->|Yes| Check2NF{Check 2NF<br/>No Partial Deps?}
    Fix1NF --> Check2NF
    
    Check2NF -->|No| Fix2NF[Extract Partial<br/>Dependencies]
    Check2NF -->|Yes| Check3NF{Check 3NF<br/>No Transitive Deps?}
    Fix2NF --> Check3NF
    
    Check3NF -->|No| Fix3NF[Extract Transitive<br/>Dependencies]
    Check3NF -->|Yes| AddKeys[Add Surrogate<br/>Keys if Needed]
    Fix3NF --> AddKeys
    
    AddKeys --> Done[✅ 3NF Normalized<br/>Tables]
    
    style Raw fill:#FFB6C1
    style Done fill:#90EE90
    style Fix1NF fill:#FFD700
    style Fix2NF fill:#FFD700
    style Fix3NF fill:#FFD700
    style AddKeys fill:#87CEEB
```

## Foreign Key Detection Algorithm

```mermaid
graph TD
    Start[Analyze Table Pair] --> NameSim[Calculate<br/>Name Similarity]
    NameSim --> ValOverlap[Calculate<br/>Value Overlap]
    ValOverlap --> Cardinality[Detect<br/>Cardinality Pattern]
    Cardinality --> CheckPK{Referenced<br/>Column is PK?}
    
    CheckPK -->|Yes| HighScore[Score += 20]
    CheckPK -->|No| LowScore[Score += 0]
    
    HighScore --> CheckSim{Name Sim<br/>> 0.9?}
    LowScore --> CheckSim
    
    CheckSim -->|Yes| AddSim[Score += 40]
    CheckSim -->|No| AddSim2[Score += 10]
    
    AddSim --> CheckOverlap{Value Overlap<br/>> 0.8?}
    AddSim2 --> CheckOverlap
    
    CheckOverlap -->|Yes| AddOverlap[Score += 30]
    CheckOverlap -->|No| AddOverlap2[Score += 10]
    
    AddOverlap --> FinalCheck{Score > 50?}
    AddOverlap2 --> FinalCheck
    
    FinalCheck -->|Yes| FK[✅ Foreign Key<br/>Detected]
    FinalCheck -->|No| NoFK[❌ Not a<br/>Foreign Key]
    
    style Start fill:#E6F3FF
    style FK fill:#90EE90
    style NoFK fill:#FFB6C1
```

## Module Dependencies

```mermaid
graph TD
    Main[main.py] --> LG[langgraph_app.py]
    
    LG --> ME[metadata_extractor.py]
    LG --> AP[auto_profiler.py]
    LG --> FK[fk_detector.py]
    LG --> NM[normalizer.py]
    LG --> SG[sql_generator.py]
    LG --> UT[utils.py]
    
    AP --> ME
    FK --> ME
    FK --> AP
    NM --> ME
    NM --> AP
    NM --> FK
    SG --> ME
    SG --> AP
    SG --> FK
    SG --> NM
    
    style Main fill:#FFD700
    style LG fill:#FF6347
    style ME fill:#87CEEB
    style AP fill:#DDA0DD
    style FK fill:#DDA0DD
    style NM fill:#98FB98
    style SG fill:#FFA500
    style UT fill:#F0E68C
```

## System Architecture

```mermaid
graph TB
    subgraph "Input Layer"
        CSV[CSV Files]
        JSON[JSON Files]
    end
    
    subgraph "Processing Layer"
        Extract[Metadata<br/>Extraction]
        Profile[Data<br/>Profiling]
        Detect[Key<br/>Detection]
        Normalize[3NF<br/>Normalization]
    end
    
    subgraph "Generation Layer"
        SQLGen[SQL<br/>Generation]
        ERDGen[ERD<br/>Generation]
    end
    
    subgraph "Output Layer"
        NormCSV[Normalized<br/>CSV/JSON]
        SQLScript[SQL DDL<br/>Scripts]
        ERDImage[ERD<br/>Diagrams]
    end
    
    subgraph "Orchestration"
        LangGraph[LangGraph<br/>Workflow Engine]
    end
    
    CSV --> Extract
    JSON --> Extract
    
    Extract --> Profile
    Profile --> Detect
    Detect --> Normalize
    
    Normalize --> SQLGen
    Normalize --> ERDGen
    
    SQLGen --> SQLScript
    ERDGen --> ERDImage
    Normalize --> NormCSV
    
    LangGraph -.controls.-> Extract
    LangGraph -.controls.-> Profile
    LangGraph -.controls.-> Detect
    LangGraph -.controls.-> Normalize
    LangGraph -.controls.-> SQLGen
    LangGraph -.controls.-> ERDGen
    
    style LangGraph fill:#FF6347
    style Extract fill:#87CEEB
    style Profile fill:#DDA0DD
    style Detect fill:#DDA0DD
    style Normalize fill:#98FB98
    style SQLGen fill:#FFA500
    style ERDGen fill:#F0E68C
```

## To view these diagrams:

1. **Copy the Mermaid code** from this file
2. **Paste into** https://mermaid.live/
3. **Or use VS Code** with Mermaid preview extension

---

*These diagrams provide visual representations of the system's workflow, architecture, and algorithms.*

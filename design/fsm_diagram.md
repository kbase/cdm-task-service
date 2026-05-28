# Job State FSM

> **Viewing:** Install the VS Code extension
> [Markdown Preview Mermaid Support](https://marketplace.visualstudio.com/items?itemName=bierner.markdown-mermaid)
> and press `Ctrl+Shift+V`, or paste the diagram block into [mermaid.live](https://mermaid.live).

Dashed arrows are **representative**: the arrow originates from one state for diagram
clarity; the label describes the full set of valid source states.

```mermaid
flowchart TD
    s(( )) --> CREATED

    %% ── Happy path ──────────────────────────────────────────────────
    CREATED --> DS[DOWNLOAD_SUBMITTED]
    DS      --> JSIng[JOB_SUBMITTING]
    JSIng   --> JSd[JOB_SUBMITTED]
    JSd     --> USIng[UPLOAD_SUBMITTING]
    USIng   --> USd[UPLOAD_SUBMITTED]
    USd     --> COMPLETE[COMPLETE]

    %% ── Error-processing path (declared first → tends to go left) ──
    JSd    --> EPSIng[ERROR_PROCESSING_SUBMITTING]
    EPSIng --> EPSd[ERROR_PROCESSING_SUBMITTED]
    EPSd   --> ERROR[ERROR]

    %% Representative: any non-terminal state can reach ERROR directly (not CANCELING)
    JSIng -.->|"any non-terminal, non-RECOVERING, non-canceling"| ERROR

    %% ── Cancel path ─────────────────────────────────────────────────
    %% Representative: any non-terminal, non-RECOVERING state can cancel
    USd -.->|"any non-terminal, non-canceling, non-RECOVERING"| CANCELING[CANCELING]
    CANCELING --> CANCELED[CANCELED]

    %% ── Recovery ────────────────────────────────────────────────────
    %% Representative: any except COMPLETE, RECOVERING, and cancel states
    DS -.->|"any except COMPLETE, RECOVERING, canceling"| RECOVERING[RECOVERING]
    RECOVERING -->|"force (10 min cooldown)"| RECOVERING
    RECOVERING -.->|reset| DS

    %% ── Styles ──────────────────────────────────────────────────────
    classDef normal  fill:#3498db,stroke:#2471a5,color:white
    classDef term    fill:#1e8449,stroke:#145a32,color:white
    classDef errTerm fill:#c0392b,stroke:#7b241c,color:white
    classDef canTerm fill:#7d3c98,stroke:#6c3483,color:white
    classDef eps     fill:#e67e22,stroke:#ca6f1e,color:white
    classDef rec     fill:#1a6fa5,stroke:#154f78,color:white
    classDef canNode fill:#e74c3c,stroke:#c0392b,color:white

    class CREATED,DS,JSIng,JSd,USIng,USd normal
    class COMPLETE term
    class ERROR errTerm
    class CANCELED canTerm
    class EPSIng,EPSd eps
    class RECOVERING rec
    class CANCELING canNode
```

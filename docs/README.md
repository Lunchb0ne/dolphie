# Documentation Overview

This folder contains comprehensive documentation for Dolphie, a real-time analytics TUI for MySQL, PostgreSQL, and ProxySQL databases.

## Documents

### 1. **ARCHITECTURE.md** 📐
Complete technical architecture and design documentation covering:
- High-level system architecture diagram
- Core components (App, Dolphie, Database layers, Workers, MetricManager, TabManager)
- Detailed component descriptions and responsibilities
- UI architecture and layout
- Data flow examples
- Startup and continuous refresh flows
- Key design patterns
- Extension points

**Use this for**: Understanding how the system works, how components interact, adding new features.

### 2. **PANELS.md** 🎨
Comprehensive reference for all UI panels and widgets covering:
- UI layout overview
- TopBar and Sparkline widgets
- MySQL panels (Dashboard, Processlist, Graphs, Replication, Metadata Locks, DDL, Performance Schema, Statements Summary)
- PostgreSQL panels (Dashboard, Processlist, Graphs)
- ProxySQL panels (Dashboard, Processlist, Hostgroup Summary, Query Rules, Command Stats)
- Metric graphs system architecture
- Keyboard shortcuts
- Panel design patterns

**Use this for**: UI reference, understanding what each panel displays, keyboard shortcuts.

### 3. **POSTGRESQL_SUPPORT.md** 🐘
Detailed documentation on PostgreSQL support in Dolphie covering:
- Implementation status (completed and future features)
- Architecture and connection detection
- Data processing pipeline
- Metric manager and metrics definitions
- SQL queries used for data collection
- Dashboard layout and features
- Processlist panel details
- Metric graphs
- DML query calculation
- System utilization collection
- PostgreSQL-specific considerations
- Feature mapping (MySQL vs PostgreSQL)
- Common issues and solutions
- Testing procedures
- Future enhancement roadmap

**Use this for**: Understanding PostgreSQL support, troubleshooting PostgreSQL connections, extending PostgreSQL features.

---

## Quick Reference

### For New Developers
1. Start with **ARCHITECTURE.md** - Understand the system structure
2. Read **PANELS.md** - See what users interact with
3. Review **POSTGRESQL_SUPPORT.md** - If working with PostgreSQL

### For Adding Features
1. **New Panel?** → PANELS.md + ARCHITECTURE.md (Components section)
2. **New Database Type?** → ARCHITECTURE.md (Extension Points) + POSTGRESQL_SUPPORT.md
3. **New Metrics?** → ARCHITECTURE.md (MetricManager section) + POSTGRESQL_SUPPORT.md
4. **Bug in PostgreSQL?** → POSTGRESQL_SUPPORT.md (Common Issues)

### Key Concepts

#### Connection Source Detection
Different database types (MySQL, PostgreSQL, ProxySQL) are automatically detected and routed to appropriate handlers.

#### Per-Tab Isolation
Each database connection is a separate tab with its own metrics, state, and panels.

#### Async Refresh
Background workers fetch data without blocking the UI.

#### Metric Abstraction
MetricManager provides a consistent interface across all database types with per-second rate calculations and graph data generation.

---

## Architecture Overview (TL;DR)

```
User Input
    ↓
App/KeyEventManager
    ↓
Panel (Dashboard/Processlist/etc.)
    ↓
Reads from Dolphie instance
    ↓
WorkerManager (background)
    ├── Database.execute() → raw data
    ├── WorkerDataProcessor.process_*() → typed objects
    ├── Dolphie.global_status, processlist_threads, etc.
    ├── MetricManager.refresh_data() → per-second rates, graphs
    └── Textual re-renders changed widgets
```

---

## Panel System Overview

### MySQL Panels
1. Dashboard - Overview metrics
2. Processlist - Active connections
3. Graphs - Time-series visualizations
4. Replication - Replication status
5. Metadata Locks - Lock monitoring
6. DDL - DDL progress
7. Performance Schema Metrics - Deep-dive metrics
8. Statements Summary - Statement statistics

### PostgreSQL Panels
1. Dashboard - Overview metrics, system utilization (local only)
2. Processlist - Active backend connections
3. Graphs - Time-series visualizations

### ProxySQL Panels
1. Dashboard - ProxySQL overview
2. Processlist - Active connections
4. Hostgroup Summary - Hostgroup statistics
5. Query Rules - Routing rules
6. Command Stats - Command statistics

---

## Configuration & Usage

### Starting Dolphie

```bash
# MySQL (URI form)
dolphie mysql://user:password@localhost:3306

# ProxySQL (URI form)
dolphie proxysql://user:password@localhost:6032

# PostgreSQL (URI form)
dolphie postgresql://user:password@localhost:5432

# If you prefer flags instead of embedding credentials in a URI:
# dolphie --type postgresql -h localhost -P 5432 -u user -p password
```

Security note: URIs and CLI flags that include passwords may be captured in shell history and process lists. Dolphie also supports safer credential sources such as environment variables (`DOLPHIE_USER`, `DOLPHIE_PASSWORD`, …), `~/.mylogin.cnf` (mysql_config_editor), and `~/.my.cnf`.

### Keyboard Controls

| Key | Action |
|-----|--------|
| `1-8` | Toggle panels (ProxySQL reuses `4-6` for its panels) |
| `q` | Quit |
| `?` | Command palette |
| `f` | Filter threads |
| `c` | Clear filters |

---

## System Requirements

- Python 3.9+
- MySQL 5.7+, MariaDB 10.0+, or PostgreSQL 12+
- ProxySQL support optional
- `psutil` for system utilization collection (enabled automatically for local connections)

---

## File Structure

```
docs/
├── ARCHITECTURE.md          # Technical system design
├── PANELS.md               # UI reference guide
└── POSTGRESQL_SUPPORT.md   # PostgreSQL feature documentation
```

---

## Contributing

When adding new features:

1. **Update relevant doc**: ARCHITECTURE.md for internals, PANELS.md for UI
2. **Add examples**: Include code snippets and screenshots
3. **Document assumptions**: What versions, extensions, or features are required
4. **Keep in sync**: Update docs when refactoring code

---

## Document History

- **Created**: January 4, 2026
- **Last Updated**: January 4, 2026
- **Consolidation**: Combined 5 separate documents into 3 comprehensive guides
  - Merged PANELS_OVERVIEW + PROGRAM_FLOW → ARCHITECTURE.md
  - Merged POSTGRESQL_FEATURE_PARITY_* files → POSTGRESQL_SUPPORT.md
  - Created new PANELS.md reference guide

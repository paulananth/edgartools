# EdgarTools Examples

Learn EdgarTools with **62 examples**: 28 interactive notebooks + 9 Python scripts

## 📓 Interactive Notebooks (`notebooks/`)

**28 Jupyter notebooks** organized by topic - perfect for learning interactively:

### Quick Start
- **[beginner/](notebooks/beginner/)** - 3 notebooks: Getting started guides
- **[xbrl/](notebooks/xbrl/)** - 18 notebooks: Financial statement analysis
- **[filings/](notebooks/filings/)** - 3 notebooks: Working with SEC filings
- **[funds/](notebooks/funds/)** - 3 notebooks: Investment fund analysis
- **[insiders/](notebooks/insiders/)** - 1 notebook: Insider trading data

**[See all notebooks →](notebooks/README.md)**

### How to Use Notebooks
1. Install EdgarTools: `pip install edgartools`
2. Open any notebook in Jupyter Lab, VS Code, or Google Colab
3. Run cells to see examples in action

---

## 💻 Python Scripts (`scripts/`)

**9 production-ready scripts** you can copy and adapt:

### 🎯 Basic (`scripts/basic/`)
Simple examples for common tasks:
- **TenkText.py** - Extract text from 10-K filings
- **entity_facts_dataframe.py** - Load company facts into pandas DataFrame

### 🚀 Advanced (`scripts/advanced/`)
Complex use cases and advanced features:
- **enterprise_config.py** - Configure custom SEC mirrors and rate limiting
- **ranking_search_examples.py** - Search filings with BM25 ranking
- **section_detection_demo.py** - Extract specific sections from filings
- **start_page_number_example.py** - Work with filing page numbers

### 🤖 AI Integration (`scripts/ai/`)
Integrate EdgarTools with AI/LLM workflows:
- **ai_context.py** - Build context for AI analysis
- **basic_docs.py** - Extract documents for AI processing
- **skills_usage.py** - Use EdgarTools AI skills

### How to Use Scripts
1. Install EdgarTools: `pip install edgartools`
2. Copy any script
3. Run: `python script_name.py`

---

## 📊 Dashboards (`dashboard/`)

Interactive Streamlit dashboards over the Snowflake gold layer
(`EDGARTOOLS_DEV.EDGARTOOLS_GOLD`) produced by the dbt project under
`infra/snowflake/dbt/edgartools_gold/`.

- **[edgar_universe_dashboard.py](dashboard/edgar_universe_dashboard.py)** —
  six-section explorer with a world map of companies by country of
  incorporation, US state choropleth, industry/entity breakdowns, filing
  activity trends, insider-transaction/AUM views, and a ticker/name lookup.

See **[dashboard/README.md](dashboard/README.md)** for setup, the
`~/.snowflake/config.toml` stanza, and launch command.

---

## 📚 Additional Resources

- **[docs/](../docs/)** - Full documentation
- [EdgarTools Documentation](https://edgartools.readthedocs.io/)
- [GitHub Repository](https://github.com/dgunning/edgartools)

## Contributing

Found an issue or have an improvement? Open an issue or PR at:
https://github.com/dgunning/edgartools

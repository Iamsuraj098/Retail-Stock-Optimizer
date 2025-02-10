# Retail Stock Optimizer

## Overview
The **Retail Stock Optimizer** (On-Shelf Availability (OSA) & Out-of-Stock (OOS) Tracker) is a data-driven solution designed to monitor and analyze product availability at retail locations. It helps optimize inventory levels, reduce stockouts, and improve supply chain efficiency by leveraging data processing techniques in **PySpark**.

## Features
- **Inventory Tracking**: Monitors on-hand stock levels at various store locations.
- **Replenishment Analysis**: Calculates rolling stock levels to predict replenishment needs.
- **Lead Time Calculation**: Computes the shortest vendor lead time for each store-SKU combination.
- **Sales Velocity Tracking**: Analyzes average daily sales over a rolling window to predict future demand.
- **Data Processing**: Utilizes PySpark for efficient big data handling.

## Data Workflow
1. **Extract**: Collect inventory, sales, and vendor lead time data.
2. **Transform**:
   - Compute **prior inventory** levels and rolling stock.
   - Calculate **daily sales velocity** and replenishment trends.
   - Determine **minimum lead times** based on vendor supply chain data.
3. **Load**: Store processed data for reporting and visualization.

## Key Calculations
### Rolling Stock Calculation:
```sql
SUM(prior_inventory) OVER(PARTITION BY store_id, sku ORDER BY date ROWS BETWEEN 90 PRECEDING AND CURRENT ROW) / 
(SUM(replenishment_flag) OVER(PARTITION BY store_id, sku ORDER BY date ROWS BETWEEN 90 PRECEDING AND CURRENT ROW) + 1)
```

### Lead Time Determination:
```python
f.expr('LEAST(lead_time_in_dc, lead_time_in_transit, lead_time_on_order)')
```

## Technology Stack
- **Apache Spark (PySpark)** for large-scale data processing
- **Databricks** for cloud-based analytics
- **SQL** for querying and aggregating inventory data
- **Pandas** for additional data manipulation

## How to Run the Project
1. Load the dataset into Databricks.
2. Ensure all necessary PySpark functions are imported:
   ```python
   from pyspark.sql.types import *
   import pyspark.sql.functions as f
   import pandas as pd
   ```
3. Execute the transformations in sequence:
   - Compute inventory metrics
   - Calculate replenishment trends
   - Determine vendor lead times
4. Use `display()` to visualize results in Databricks.

## Future Enhancements
- **Machine Learning**: Implement demand forecasting models.
- **Real-Time Analytics**: Integrate streaming data for live tracking.
- **Visualization**: Develop dashboards for insights on stock levels and replenishment schedules.

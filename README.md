# CAISO Load-Shifting Outlook

This project provides an interactive visualization of day-ahead nodal locational marginal prices (LMPs) and grid stress forecasts for California ISO. It is designed to help electricity consumers understand the ideal hours to deploy flexible electrical loads to minimize grid stress.

Intended to be useful for:

-Energy management and demand response planning

-Research on grid congestion and nodal pricing

-Educational purposes for understanding the California energy market

______________________________________________________________________________________________________________
Key features include:

-Load-Shifting Outlook Tab: Highlights the hours with the lowest expected grid stress at each node using day-ahead LMPs.

-Day-Ahead LMP Map: Visualizes hourly electricity prices across 2,043 nodes in California using D3 and Leaflet.

-Automated Updates: Fetches and processes LMP data from CAISO hourly, using automated cron scheduling from Cloudflare.



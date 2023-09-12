This folder contains special scripts that provide additional analyses
for data requests.
A typical example is a "customer" wants only one country and a subset of the data
in that country.

By putting scripts to generate data for these customers
in a single place, we can look back at how we generated those data.

Files are named by the product used as the basis for the script
followed by a descriptive aspect of the report.
For example:

Product E - Austria.R

indicates that Product E is filtered to show only Austrian data.
Each file should have comments at the top indicating
the customer for whom the script was created and a date.

---Matthew Kuperus Heun, 12 Sept 2023

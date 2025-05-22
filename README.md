# HBase WebTable Case Study - Documentation 🚀

## Row Key Design Rationale 🔑

The row key is designed by reversing the domain name and appending the page path and a padded numeric suffix.  
For example, the URL `www.google.com/page/1` becomes: `com.google.www/page/001`

### Justification:
- **📅 Lexicographical Order**: Reversing the domain groups all pages under the same domain prefix, improving range scan efficiency.
- **🗂️ Path Sorting**: Including the page path maintains uniqueness while supporting structured hierarchy within the domain.
- **🔢 Padded Numeric Suffix**: Appending a zero-padded counter (e.g., `001`) enables predictable lexicographical ordering, facilitating **efficient pagination** and **batch scans**.
- **⚖️ Scalability**: The key design supports horizontal scaling and sharding across region servers based on domain-level prefixes.


### Pagination Implementation Details 📄➡️📄
The reversed domain structure enables ordered scanning using `STARTROW` and `STOPROW` with row key markers for "next page" functionality. Pagination is implemented using the **PageFilter** in HBase, which limits the number of rows returned per scan (e.g., batches of 5 pages).

Because the row key sorts pages by domain first, followed by the page path and padded page number, the **PageFilter** works efficiently within each domain prefix. This means:

- We can scan pages for a specific domain (e.g., `com.google.www/`) in **lexicographical order**.
- The number suffix in the row key ensures pages are returned in numerical order (page 001, 002, 003, etc.).
- The last row key from each scan is used as the `STARTROW` for the next scan, enabling **seamless "next page" navigation**.
- This design reduces data transfer, improves memory usage, and speeds up response time by only fetching a small batch per query.

Pagination thus leverages the natural sort order of the row key to group pages by domain and sequence, making it scalable and user-friendly.

### Weaknesses ⚠️:
- **🔥 Hotspotting Risk**: Popular domains (e.g., `com.google.www`) may receive a disproportionate number of read/write operations, leading to **region server hotspots** and load imbalance.
- **🔄 Limited Load Distribution**: Since row keys are grouped by domain, less popular domains might underutilize resources, while hot domains might saturate single regions.

## Versioning and TTL Policy Justifications ⏳

| Column Family | Versions | TTL        | Rationale                                                                 |
|---------------|----------|------------|--------------------------------------------------------------------------|
| `content`     | 3        | 90 days    | Retain multiple recent versions for content rollback and change tracking. TTL ensures stale data is auto-purged to manage storage. 📜 |
| `metadata`    | 1        | None       | Only the latest metadata (e.g., title, status) is relevant; no need for historical tracking. 🏷️ |
| `outlinks`    | 2        | 180 days   | Track how outbound links evolve over time for SEO analysis; longer TTL accounts for slower link decay. 🔗 |
| `inlinks`     | 2        | 180 days   | Important for popularity and backlink analysis; preserved over a longer window for historical insight. ⭐ |






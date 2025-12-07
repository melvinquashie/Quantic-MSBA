-- Creating Tables
--Main table with data extracted by GOG.com API in June 2025
CREATE TABLE gog_raw_inventory (
    -- 1. Identifier
    id INTEGER PRIMARY KEY,
    
    -- 2. Parties and Media
    developer TEXT,
    publisher TEXT,
    gallery TEXT,
    video TEXT,
    
    -- 3. Compatibility & Categories
    supportedOperatingSystems TEXT,
    genres TEXT,
    
    -- 4. Release Dates (All TEXT for safety)
    globalReleaseDate TEXT,
    isTBA TEXT,
    isDiscounted_main TEXT,
    isInDevelopment TEXT,
    releaseDate TEXT,
    
    -- 5. Availability & Visibility
    availability TEXT,
    salesVisibility TEXT,
    buyable TEXT,
    
    -- 6. Title Information & URLs
    title TEXT NOT NULL,
    image TEXT,
    url TEXT,
    supportUrl TEXT,
    forumUrl TEXT,
    worksOn TEXT,
    
    -- 7. Secondary Categories & Type
    category TEXT,
    originalCategory TEXT,
    rating TEXT,
    type INTEGER,
    
    -- 8. Status Flags
    isComingSoon TEXT,
    isPriceVisible TEXT,
    isMovie TEXT,
    isGame TEXT,
    slug TEXT,
    isWishlistable TEXT,
    ageLimit TEXT,
    boxImage TEXT,
    isMod TEXT,
    
    -- 9. Pricing and Discounts
    currency TEXT,
    amount TEXT,
    baseAmount TEXT,
    finalAmount TEXT,
    isDiscounted TEXT,
    discountPercentage TEXT,
    discountDifference TEXT,
    discount TEXT,
    isFree TEXT,
    promoId TEXT,
    
    -- 10. Ratings and Reviews
    filteredAvgRating TEXT,
    overallAvgRating TEXT,
    reviewsCount TEXT,
    isReviewable TEXT,
    reviewPages TEXT,
    
    -- 11. Date Objects
    dateGlobal TEXT,
    dateReleaseDate TEXT
);
-- Populating the table
COPY gog_raw_inventory
FROM '/tmp/gog_games_dataset.csv' 
DELIMITER ',' 
CSV 
HEADER; -- This tells the system to skip the first row (column names)

--validating that it worked
SELECT * FROM gog_raw_inventory;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Creating core entity tables

-- Party table
CREATE TABLE party (
    party_id SERIAL PRIMARY KEY, -- SERIAL automatically generates unique IDs
    name TEXT UNIQUE NOT NULL
);
-- populating the party table
-- Step A: Insert all unique developer names
INSERT INTO party (name)
SELECT DISTINCT developer
FROM gog_raw_inventory
WHERE developer IS NOT NULL
ON CONFLICT (name) DO NOTHING;
-- Step B: Insert all unique publisher names (which may include names already inserted in Step A)
INSERT INTO party (name)
SELECT DISTINCT publisher
FROM gog_raw_inventory
WHERE publisher IS NOT NULL
ON CONFLICT (name) DO NOTHING;

--validating that it worked
SELECT * FROM party;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Product_Type Table
CREATE TABLE product_type (
    type_id INTEGER PRIMARY KEY,
    type_name TEXT NOT NULL
);
-- Populate the table with static definitions
INSERT INTO product_type (type_id, type_name) VALUES
(1, 'Game'),
(2, 'Bundle'),
(3, 'DLC');

--validating that it worked
SELECT * FROM product_type;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Inventory table
CREATE TABLE inventory (
    title_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    slug TEXT,
    release_date TIMESTAMP, 
    age_limit INTEGER,
    is_game BOOLEAN,
    is_movie BOOLEAN,
    buyable BOOLEAN,
    support_url TEXT,

    -- Foreign Keys (FKs) linking to other entities
    publisher_id INTEGER,
    developer_id INTEGER,
    type_id INTEGER,

    -- Foreign Key Constraints
    FOREIGN KEY (publisher_id) REFERENCES party(party_id),
    FOREIGN KEY (developer_id) REFERENCES party(party_id),
    FOREIGN KEY (type_id) REFERENCES product_type(type_id)
);
-- Populating the inventory table
INSERT INTO inventory (
    title_id, title, slug, release_date, age_limit, is_game, is_movie, buyable, support_url,
    publisher_id, developer_id, type_id
)
SELECT
    -- Identity and Core Details
    r.id::INTEGER AS title_id,
    r.title,
    r.slug,
    -- Convert raw timestamp (TEXT) to TIMESTAMP
    CASE WHEN r.globalReleaseDate IS NOT NULL AND r.globalReleaseDate != ''
         THEN TO_TIMESTAMP(r.globalReleaseDate::BIGINT) 
         ELSE NULL 
    END AS release_date,
    r.ageLimit::INTEGER AS age_limit, -- Convert TEXT to INTEGER

    -- Boolean Flags (Convert raw TEXT 't'/'f' to BOOLEAN)
    CASE WHEN r.isGame = 't' THEN TRUE ELSE FALSE END AS is_game,
    CASE WHEN r.isMovie = 't' THEN TRUE ELSE FALSE END AS is_movie,
    CASE WHEN r.buyable = 't' THEN TRUE ELSE FALSE END AS buyable,
    
    r.supportUrl,
    
    -- Foreign Keys (Look up the IDs using the PARTY table)
    p_pub.party_id AS publisher_id,
    p_dev.party_id AS developer_id,
    r.type::INTEGER AS type_id 

FROM gog_raw_inventory r
-- Join to look up Publisher ID
LEFT JOIN party p_pub ON r.publisher = p_pub.name
-- Join to look up Developer ID
LEFT JOIN party p_dev ON r.developer = p_dev.name
-- Filter out records where the ID is missing or invalid
WHERE r.id IS NOT NULL;

--validating that it worked
SELECT * FROM inventory;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- OS_Platform Table
CREATE TABLE os_platform (
    os_id SERIAL PRIMARY KEY, -- SERIAL automatically generates unique IDs
    os_name TEXT UNIQUE NOT NULL
);
-- populating the platform table
INSERT INTO os_platform (os_name)
-- We need to select all unique OS names from the raw data
SELECT DISTINCT
    -- 2. Use jsonb_array_elements_text to unnest the array into separate rows
    jsonb_array_elements_text(
        -- 1. Convert the raw text string (which looks like a list) into a valid JSONB array
        REPLACE(REPLACE(TRIM(r.supportedOperatingSystems), '''', '"'), '""', '"') :: jsonb
    ) AS os_name
FROM gog_raw_inventory r
-- Ensure we only process rows that are not empty or null
WHERE r.supportedOperatingSystems IS NOT NULL 
  AND TRIM(r.supportedOperatingSystems) != '[]'
  AND TRIM(r.supportedOperatingSystems) != '';

--validating that it worked
SELECT * FROM os_platform;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Genre Table
CREATE TABLE genre (
    genre_id SERIAL PRIMARY KEY, -- SERIAL automatically generates unique IDs
    genre_name TEXT UNIQUE NOT NULL
);
-- Populating the genre table
INSERT INTO genre (genre_name)
SELECT DISTINCT 
    -- 4. Trim any remaining spaces and convert to lowercase for better grouping (e.g., 'Action' and 'action' are treated the same)
    TRIM(LOWER(genre_name_unnested))
FROM (
    SELECT 
        -- 3. Use REGEXP_SPLIT_TO_TABLE to turn the cleaned string into multiple rows, splitting by comma and optional spaces
        REGEXP_SPLIT_TO_TABLE(
            -- 2. Clean the string: remove brackets ([]), remove single quotes, and remove double quotes
            REPLACE(REPLACE(REPLACE(TRIM(r.genres), '[', ''), ']', ''), '''', ''), 
            ',\s*' -- This pattern splits the text by a comma followed by any number of spaces
        ) AS genre_name_unnested
    FROM gog_raw_inventory r
    -- 1. Ensure we only process non-empty fields
    WHERE r.genres IS NOT NULL 
      AND TRIM(r.genres) NOT IN ('', '[]')
) AS unnested_genres
-- Final check to ensure we don't insert empty names
WHERE genre_name_unnested IS NOT NULL AND TRIM(genre_name_unnested) != '';
  
--validating that it worked
SELECT * FROM genre;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Promotion Table
CREATE TABLE promotion (
    promo_id SERIAL PRIMARY KEY, -- SERIAL automatically generates unique IDs
    promo_name TEXT UNIQUE NOT NULL
);
-- Populating promotion table
INSERT INTO promotion (promo_name)
SELECT DISTINCT TRIM(promoId)
FROM gog_raw_inventory
WHERE promoId IS NOT NULL 
  AND TRIM(promoId) != '';

--validating that it worked
SELECT * FROM promotion;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Information/Status tables

-- Pricing table
CREATE TABLE pricing (
    title_id INTEGER PRIMARY KEY,
    base_amount NUMERIC,
    final_amount NUMERIC,
    amount NUMERIC,
    currency TEXT,
    is_free BOOLEAN,
    
    -- Links back to the inventory table
    FOREIGN KEY (title_id) REFERENCES inventory(title_id)
);
-- Ppulating the pricing table
INSERT INTO pricing (
    title_id, base_amount, final_amount, amount, currency, is_free
)
SELECT
    r.id::INTEGER AS title_id,
    -- Convert raw TEXT price fields to NUMERIC
    NULLIF(r.baseAmount, '')::NUMERIC,
    NULLIF(r.finalAmount, '')::NUMERIC,
    NULLIF(r.amount, '')::NUMERIC,
    r.currency,
	-- *** REVISED LOGIC FOR is_free ***
    CASE 
        -- Rule 1: If base price, final price, AND current price are all 0
        WHEN NULLIF(r.baseAmount, '')::NUMERIC = 0 
         AND NULLIF(r.finalAmount, '')::NUMERIC = 0 
         AND NULLIF(r.amount, '')::NUMERIC = 0 
        THEN TRUE
        
        -- Rule 2 (Fallback): Use the raw isFree flag from the source data
        WHEN r.isFree = 't' THEN TRUE
        
        -- Default: Otherwise, it is not free
        ELSE FALSE
    END AS is_free
FROM gog_raw_inventory r
-- Only insert pricing data for titles that successfully made it into the clean inventory table
WHERE r.id IN (SELECT title_id FROM inventory);

--validating that it worked
SELECT * FROM pricing;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Rating Table
CREATE TABLE rating (
    title_id INTEGER PRIMARY KEY,
    overall_avg_rating NUMERIC,      -- For overallAvgRating (e.g., 4.5)
    filtered_avg_rating NUMERIC,     -- For filteredAvgRating
    reviews_count INTEGER,           -- For number of reviews
    is_reviewable BOOLEAN,
    review_pages INTEGER,
    
    -- Links back to the inventory table
    FOREIGN KEY (title_id) REFERENCES inventory(title_id)
);
-- Populating the review table
INSERT INTO rating (
    title_id, overall_avg_rating, filtered_avg_rating, reviews_count, is_reviewable, review_pages
)
SELECT
    r.id::INTEGER AS title_id,
    
    -- Cast raw TEXT fields to NUMERIC
    NULLIF(r.overallAvgRating, '')::NUMERIC AS overall_avg_rating,
    NULLIF(r.filteredAvgRating, '')::NUMERIC AS filtered_avg_rating,
    
    -- Cast raw TEXT fields to INTEGER
    NULLIF(r.reviewsCount, '')::INTEGER AS reviews_count,
    
    -- Convert raw TEXT 't'/'f' to BOOLEAN
    CASE WHEN r.isReviewable = 't' THEN TRUE ELSE FALSE END AS is_reviewable,
    
    NULLIF(r.reviewPages, '')::INTEGER AS review_pages
    
FROM gog_raw_inventory r
-- Only insert ratings data for titles that exist in the clean inventory
WHERE r.id IN (SELECT title_id FROM inventory);

--validating that it worked
SELECT * FROM rating;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Discount_status Table
CREATE TABLE discount_status (
    -- Composite Primary Key
    title_id INTEGER,
    promo_id INTEGER,
    
    discount_percentage NUMERIC,
    discount_difference NUMERIC,
    
    is_discounted BOOLEAN,
    
    PRIMARY KEY (title_id, promo_id),
    
    -- Foreign Key Constraints
    FOREIGN KEY (title_id) REFERENCES inventory(title_id),
    FOREIGN KEY (promo_id) REFERENCES promotion(promo_id)
);
--Populating the discount_status table
INSERT INTO discount_status (
    title_id, promo_id, discount_percentage, discount_difference, is_discounted
)
SELECT
    r.id::INTEGER AS title_id,
    p.promo_id,
    
    -- Clean and cast numeric fields (required to prevent 'FALSE' being cast as a number)
    CASE WHEN UPPER(r.discountPercentage) IN ('TRUE', 'FALSE') 
         THEN NULL 
         ELSE NULLIF(r.discountPercentage, '')::NUMERIC 
    END AS discount_percentage,
    
    CASE WHEN UPPER(r.discountDifference) IN ('TRUE', 'FALSE') 
         THEN NULL 
         ELSE NULLIF(r.discountDifference, '')::NUMERIC 
    END AS discount_difference,
    
-- *** REVISED is_discounted LOGIC ***
    CASE 
        -- Rule 1: Set to TRUE if the discount percentage is positive (your request)
        WHEN NULLIF(r.discountPercentage, '')::NUMERIC > 0 THEN TRUE
        
        -- Rule 2 (Fallback): Use the raw 't' flag from the source data
        WHEN TRIM(r.isDiscounted) = 't' THEN TRUE
        
        -- Default: Rows with 0% discount, NULL, or 'f' are marked FALSE
        ELSE FALSE
    END AS is_discounted
    
FROM gog_raw_inventory r
-- INNER JOIN ensures rows without a valid promoId are excluded (required by the FK constraint)
INNER JOIN promotion p ON TRIM(r.promoId) = p.promo_name
WHERE r.id IN (SELECT title_id FROM inventory);

--validating that it worked
SELECT * FROM discount_status;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Junction tables
-- Title_Genre table
CREATE TABLE title_genre (
    -- Composite Primary Key made of two Foreign Keys
    title_id INTEGER,
    genre_id INTEGER,
    
    PRIMARY KEY (title_id, genre_id),
    
    -- Foreign Key Constraints
    FOREIGN KEY (title_id) REFERENCES inventory(title_id),
    FOREIGN KEY (genre_id) REFERENCES genre(genre_id)
);
-- populating the title_genre table using text split fix
INSERT INTO title_genre (title_id, genre_id)
SELECT DISTINCT
    r.id::INTEGER AS title_id,
    g.genre_id
FROM gog_raw_inventory r
-- Use the robust text splitting method to unnest the genre names
CROSS JOIN LATERAL (
    SELECT REGEXP_SPLIT_TO_TABLE(
        REPLACE(REPLACE(REPLACE(TRIM(r.genres), '[', ''), ']', ''), '''', ''), 
        ',\s*'
    ) AS genre_name_unnested
) AS unnested_genre(genre_name_unnested)
-- Join to the clean GENRE table to get the genre_id
INNER JOIN genre g ON g.genre_name = TRIM(LOWER(unnested_genre.genre_name_unnested))
WHERE r.id IN (SELECT title_id FROM inventory);

--validating that it worked
SELECT * FROM title_genre;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Title_OS table
CREATE TABLE title_os (
    -- Composite Primary Key made of two Foreign Keys
    title_id INTEGER,
    os_id INTEGER,
    
    PRIMARY KEY (title_id, os_id),
    
    -- Foreign Key Constraints
    FOREIGN KEY (title_id) REFERENCES inventory(title_id),
    FOREIGN KEY (os_id) REFERENCES os_platform(os_id)
);
-- populating the title_os table
INSERT INTO title_os (title_id, os_id)
SELECT DISTINCT
    r.id::INTEGER AS title_id,
    osp.os_id
FROM gog_raw_inventory r
-- Use the same robust parsing to unnest the OS array data
CROSS JOIN LATERAL (
    SELECT jsonb_array_elements_text(
        REPLACE(REPLACE(TRIM(r.supportedOperatingSystems), '''', '"'), '""', '"') :: jsonb
    ) AS os_name_unnested
) AS unnested_os(os_name_unnested)
-- Join to the clean OS_PLATFORM table to get the os_id
INNER JOIN os_platform osp ON osp.os_name = unnested_os.os_name_unnested
WHERE r.id IN (SELECT title_id FROM inventory);

--validating that it worked
SELECT * FROM title_os;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Analysis

-- Which Developers and Publishers are the most successful, based on median user ratings and catalog presence?
-- Query 1: Top Developers by Median User Rating and Catalog Volume
SELECT
    P.name AS developer_name,
    COUNT(T.title_id) AS catalog_presence,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY R.overall_avg_rating) AS median_rating
FROM inventory T
JOIN party P ON T.developer_id = P.party_id
JOIN rating R ON T.title_id = R.title_id
WHERE R.overall_avg_rating IS NOT NULL
GROUP BY P.name
HAVING COUNT(T.title_id) >= 5 -- Filter for developers with at least 5 titles
ORDER BY median_rating DESC, catalog_presence DESC;
-- Query 2: Top Publishers by Median User Rating and Catalog Volume
SELECT
    P.name AS publisher_name,
    COUNT(T.title_id) AS catalog_presence,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY R.overall_avg_rating) AS median_rating
FROM inventory T
JOIN party P ON T.publisher_id = P.party_id
JOIN rating R ON T.title_id = R.title_id
WHERE R.overall_avg_rating IS NOT NULL
GROUP BY P.name
HAVING COUNT(T.title_id) >= 5 -- Filter for publishers with at least 5 titles
ORDER BY median_rating DESC, catalog_presence DESC;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- How does the quality (average rating) of Classic Games compare to modern releases, and what is the dollar value of preserving those older titles?
SELECT
    -- Step 1: Classify the game based on the 2010 cutoff date
    CASE 
        WHEN T.release_date < '2010-01-01'::DATE THEN 'Classic (Pre-2010)'
        ELSE 'Modern (2010+)'
    END AS game_era,
    COUNT(T.title_id) AS titles_in_group,
    
    -- Step 2: Assess Quality
    AVG(R.overall_avg_rating) AS avg_quality_rating,
    
    -- Step 3: Assess Dollar Value (using the original price/MSRP)
    AVG(P.base_amount) AS avg_base_price
    
FROM inventory T
JOIN rating R ON T.title_id = R.title_id
LEFT JOIN pricing P ON T.title_id = P.title_id
WHERE R.overall_avg_rating IS NOT NULL
GROUP BY game_era
ORDER BY avg_quality_rating DESC;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--How does the required Age Limit for a title relate to its genre and overall review count (a measure of engagement)?
-- Query 1: Average Age Limit per Genre
SELECT
    G.genre_name,
    COUNT(T.title_id) AS title_count,
    AVG(T.age_limit) AS avg_age_limit
FROM inventory T
JOIN title_genre TG ON T.title_id = TG.title_id
JOIN genre G ON TG.genre_id = G.genre_id
WHERE T.age_limit IS NOT NULL AND T.age_limit > 0 -- Focus only on games with an explicit age limit
GROUP BY G.genre_name
HAVING COUNT(T.title_id) >= 5 -- Require at least 5 titles for genre average to be meaningful
ORDER BY avg_age_limit DESC;

-- Query 2: Total Engagement (Reviews) by Age Restriction Tier
SELECT
    CASE
        -- Segment the age limits into business-relevant groups
        WHEN T.age_limit = 0 THEN '0 (No Restriction/Unknown)'
        WHEN T.age_limit <= 12 THEN '1-12 (Child/Pre-Teen)'
        WHEN T.age_limit BETWEEN 13 AND 16 THEN '13-16 (Teen)'
        WHEN T.age_limit >= 17 THEN '17+ (Mature Audience)'
        ELSE 'N/A'
    END AS age_restriction_group,
    COUNT(T.title_id) AS game_count,
    SUM(R.reviews_count) AS total_reviews_sum -- Measure of overall audience engagement
FROM inventory T
JOIN rating R ON T.title_id = R.title_id
WHERE T.age_limit IS NOT NULL AND R.reviews_count IS NOT NULL
GROUP BY age_restriction_group
ORDER BY total_reviews_sum DESC;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- What are the sales and popularity contribution of Bundles and DLC compared to full game titles?
SELECT
    PT.type_name AS product_category,
    
    -- Catalog Contribution (Volume)
    COUNT(T.title_id) AS catalog_count,
    
    -- Popularity Contribution (Proxy for sales volume)
    SUM(R.reviews_count) AS total_reviews_proxy,
    
    -- Revenue Contribution (Proxy for MSRP Value)
    SUM(P.base_amount) AS total_base_amount_proxy
    
FROM inventory T
JOIN product_type PT ON T.type_id = PT.type_id
LEFT JOIN rating R ON T.title_id = R.title_id
LEFT JOIN pricing P ON T.title_id = P.title_id
GROUP BY PT.type_name
ORDER BY total_reviews_proxy DESC;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--What share of the catalog revenue and inventory is derived from non-game content (like Movies, or Bundles that include extras)?
SELECT
    -- Step 1: Define specific content groups for clear segmentation
    CASE
        WHEN T.is_movie = TRUE THEN 'Movie Content'
        WHEN PT.type_name = 'Bundle' THEN 'Bundle (Package)'
        WHEN PT.type_name = 'DLC' THEN 'DLC/Add-on Content'
        WHEN PT.type_name = 'Game' THEN 'Full Game Title'
        ELSE 'Other/Unknown Content'
    END AS content_type_group,
    
    -- Step 2: Inventory Share (Volume)
    COUNT(T.title_id) AS titles_in_group,
    
    -- Step 3: Potential Revenue Share (Total MSRP Value)
    SUM(P.base_amount) AS total_base_amount_value
    
FROM inventory T
JOIN product_type PT ON T.type_id = PT.type_id
LEFT JOIN pricing P ON T.title_id = P.title_id
GROUP BY content_type_group
ORDER BY titles_in_group DESC;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--How do Free-to-Play titles perform in terms of user ratings and reviews compared to paid titles, determining if they are positive entry points for users?
SELECT
    -- Step 1: Classify titles based on the IS_FREE flag in the pricing table
    CASE
        WHEN P.is_free = TRUE THEN 'Free-to-Play (F2P)'
        WHEN P.is_free = FALSE THEN 'Paid/Premium Title'
        ELSE 'Unknown/Unlisted Price'
    END AS revenue_model,
    
    COUNT(T.title_id) AS title_count,
    
    -- Step 2: Quality Metric (Average User Rating)
    AVG(R.overall_avg_rating) AS avg_user_rating,
    
    -- Step 3: Engagement Metric (Total Reviews)
    SUM(R.reviews_count) AS total_reviews_sum
    
FROM inventory T
JOIN pricing P ON T.title_id = P.title_id
JOIN rating R ON T.title_id = R.title_id
WHERE R.overall_avg_rating IS NOT NULL
GROUP BY revenue_model
ORDER BY total_reviews_sum DESC;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- How does the average price of brand New Releases compare to the established price of the older Back Catalog?
SELECT
    -- Step 1: Classify titles using the 2-year cutoff date (June 1, 2023)
    CASE
        WHEN T.release_date >= '2023-06-01'::DATE THEN 'New Release (Last 2 Years)'
        WHEN T.release_date < '2023-06-01'::DATE THEN 'Back Catalog'
        ELSE 'Unknown/Unlisted Date'
    END AS release_category,
    
    COUNT(T.title_id) AS title_count,
    
    -- Step 2: Average Established Price (MSRP)
    AVG(P.base_amount) AS avg_base_price,
    
    -- Step 3: Average Current Price (Current Selling Price)
    AVG(P.final_amount) AS avg_final_price
    
FROM inventory T
JOIN pricing P ON T.title_id = P.title_id
WHERE P.base_amount IS NOT NULL AND P.base_amount > 0 -- Exclude free/unpriced games
GROUP BY release_category
ORDER BY avg_base_price DESC;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--How consistently is the base price set across different Currencies (regions) in the dataset?
SELECT
    currency,
    COUNT(title_id) AS title_count,
    
    -- Consistency Metric: Average established price
    AVG(base_amount) AS avg_base_price_msrp,
    
    -- Discounting Context: Average current selling price
    AVG(final_amount) AS avg_final_price_current
    
FROM pricing
WHERE base_amount IS NOT NULL AND base_amount > 0 -- Focus only on titles with an established price
GROUP BY currency
ORDER BY title_count DESC; -- Order by volume to focus on primary markets

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--Discount Depth: Are specific, named Promotions (using promoId) more effective at driving engagement than just offering the largest percentage discount?
SELECT
    P.promo_name,
    COUNT(D.title_id) AS titles_in_promotion,
    
    -- Metric 1: Discount Depth
    AVG(D.discount_percentage) AS avg_discount_depth,
    
    -- Metric 2: Engagement (Proxy for marketing effectiveness)
    SUM(R.reviews_count) AS total_reviews_driven
FROM promotion P
JOIN discount_status D ON P.promo_id = D.promo_id
JOIN rating R ON D.title_id = R.title_id
WHERE R.reviews_count IS NOT NULL -- Focus on games with quantifiable engagement
GROUP BY P.promo_name
HAVING COUNT(D.title_id) >= 10 -- Filter out campaigns featuring fewer than 10 titles for clarity
ORDER BY total_reviews_driven DESC;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--Does wide OS Support (Windows, Mac, Linux) correlate with higher prices or better user satisfaction/ratings?

WITH PlatformCounts AS (
    -- Step 1: Count the number of supported OS for each unique title
    SELECT
        title_id,
        COUNT(os_id) AS num_platforms
    FROM title_os
    GROUP BY title_id
)
SELECT
    T1.num_platforms,
    COUNT(T1.title_id) AS game_count,
    
    -- Metric 1: Average Price (Higher prices suggest platform support is seen as added value)
    AVG(P.base_amount) AS avg_base_price,
    
    -- Metric 2: Average Rating (Better quality suggests cross-platform efforts are successful)
    AVG(R.overall_avg_rating) AS avg_quality_rating
    
FROM PlatformCounts T1
JOIN inventory T ON T1.title_id = T.title_id
LEFT JOIN pricing P ON T1.title_id = P.title_id
LEFT JOIN rating R ON T1.title_id = R.title_id
WHERE P.base_amount IS NOT NULL AND R.overall_avg_rating IS NOT NULL
GROUP BY T1.num_platforms
ORDER BY T1.num_platforms DESC;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--Is there a link between lower customer ratings and the absence of a dedicated Support URL provided by the developer or publisher?
WITH TargetCategories AS (
    -- Step 1: Define the two required rows (categories) that MUST appear in the output
    SELECT 'Dedicated Support URL Present' AS support_status
    UNION ALL
    SELECT 'Support URL Missing/Not Provided'
),
AggregatedResults AS (
    -- Step 2: Aggregate the actual data based on your robust classification logic
    SELECT
        CASE
            WHEN T.support_url IS NULL 
                 OR TRIM(T.support_url) = '' 
                 OR LOWER(TRIM(T.support_url)) IN ('n/a', 'none', 'tbd') 
            THEN 'Support URL Missing/Not Provided'
            ELSE 'Dedicated Support URL Present'
        END AS status,
        COUNT(T.title_id) AS game_count,
        AVG(R.overall_avg_rating) AS avg_customer_rating,
        SUM(R.reviews_count) AS total_reviews_sum
    FROM inventory T
    JOIN rating R ON T.title_id = R.title_id
    WHERE R.overall_avg_rating IS NOT NULL
    GROUP BY status
)
SELECT
    TC.support_status,
    -- Step 3: Use COALESCE to replace NULL count (from the LEFT JOIN) with 0
    COALESCE(AR.game_count, 0) AS game_count,
    AR.avg_customer_rating, 
    AR.total_reviews_sum    
FROM TargetCategories TC
-- Step 4: LEFT JOIN ensures both TargetCategories rows are included
LEFT JOIN AggregatedResults AR ON TC.support_status = AR.status
ORDER BY TC.support_status DESC;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


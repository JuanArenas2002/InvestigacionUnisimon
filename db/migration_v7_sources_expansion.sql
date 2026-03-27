-- =============================================================
-- Migración v7: Expansión de campos en tablas de fuentes
-- Ejecutar UNA sola vez sobre la base de datos existente.
-- Todos los ALTER son idempotentes (IF NOT EXISTS).
-- =============================================================


-- ──────────────────────────────────────────────────────────────
-- openalex_records
-- ──────────────────────────────────────────────────────────────

ALTER TABLE openalex_records
    ADD COLUMN IF NOT EXISTS abstract             TEXT,
    ADD COLUMN IF NOT EXISTS keywords             TEXT,
    ADD COLUMN IF NOT EXISTS concepts             JSONB,
    ADD COLUMN IF NOT EXISTS topics               JSONB,
    ADD COLUMN IF NOT EXISTS mesh_terms           JSONB,
    ADD COLUMN IF NOT EXISTS oa_url               VARCHAR(1000),
    ADD COLUMN IF NOT EXISTS pdf_url              VARCHAR(1000),
    ADD COLUMN IF NOT EXISTS license              VARCHAR(100),
    ADD COLUMN IF NOT EXISTS referenced_works_count INTEGER,
    ADD COLUMN IF NOT EXISTS apc_paid_usd         INTEGER,
    ADD COLUMN IF NOT EXISTS grants               JSONB;

COMMENT ON COLUMN openalex_records.abstract              IS 'Resumen reconstruido desde abstract_inverted_index';
COMMENT ON COLUMN openalex_records.keywords              IS 'Palabras clave (campo keywords de OpenAlex 2024+)';
COMMENT ON COLUMN openalex_records.concepts              IS '[{display_name, score, wikidata_id, level}]';
COMMENT ON COLUMN openalex_records.topics                IS '[{display_name, score, domain, field, subfield}]';
COMMENT ON COLUMN openalex_records.mesh_terms            IS 'Medical Subject Headings cuando viene de PubMed';
COMMENT ON COLUMN openalex_records.oa_url                IS 'URL directa a versión open access';
COMMENT ON COLUMN openalex_records.pdf_url               IS 'URL directa al PDF';
COMMENT ON COLUMN openalex_records.license               IS 'Licencia: cc-by, cc-by-nc, publisher-specific-oa, etc.';
COMMENT ON COLUMN openalex_records.referenced_works_count IS 'Cantidad de referencias bibliográficas';
COMMENT ON COLUMN openalex_records.apc_paid_usd          IS 'Cargo por procesamiento de artículo (APC) en USD';
COMMENT ON COLUMN openalex_records.grants                IS '[{funder, funder_display_name, award_id}]';


-- ──────────────────────────────────────────────────────────────
-- scopus_records
-- ──────────────────────────────────────────────────────────────

ALTER TABLE scopus_records
    ADD COLUMN IF NOT EXISTS eid                  VARCHAR(100),
    ADD COLUMN IF NOT EXISTS pmid                 VARCHAR(50),
    ADD COLUMN IF NOT EXISTS isbn                 VARCHAR(30),
    ADD COLUMN IF NOT EXISTS index_keywords       TEXT,
    ADD COLUMN IF NOT EXISTS subtype_description  VARCHAR(100),
    ADD COLUMN IF NOT EXISTS conference_name      VARCHAR(500),
    ADD COLUMN IF NOT EXISTS funding_agency       VARCHAR(300),
    ADD COLUMN IF NOT EXISTS funding_number       VARCHAR(200);

CREATE INDEX IF NOT EXISTS ix_scopus_eid ON scopus_records (eid);

COMMENT ON COLUMN scopus_records.eid                 IS 'Electronic ID: 2-s2.0-XXXXXXXX';
COMMENT ON COLUMN scopus_records.pmid                IS 'PubMed ID cruzado desde Scopus';
COMMENT ON COLUMN scopus_records.isbn                IS 'ISBN para libros y capítulos de libro';
COMMENT ON COLUMN scopus_records.index_keywords      IS 'Vocabulario controlado de Scopus (idxterms)';
COMMENT ON COLUMN scopus_records.subtype_description IS 'Article, Review, Conference Paper, Book Chapter, etc.';
COMMENT ON COLUMN scopus_records.conference_name     IS 'Nombre de la conferencia (para conference papers)';
COMMENT ON COLUMN scopus_records.funding_agency      IS 'Entidad financiadora reportada en Scopus';
COMMENT ON COLUMN scopus_records.funding_number      IS 'Número o código del grant de financiación';


-- ──────────────────────────────────────────────────────────────
-- wos_records  (tabla más escasa — agrega campos críticos)
-- ──────────────────────────────────────────────────────────────

ALTER TABLE wos_records
    ADD COLUMN IF NOT EXISTS accession_number          VARCHAR(100),
    ADD COLUMN IF NOT EXISTS pmid                      VARCHAR(50),
    ADD COLUMN IF NOT EXISTS volume                    VARCHAR(50),
    ADD COLUMN IF NOT EXISTS issue                     VARCHAR(50),
    ADD COLUMN IF NOT EXISTS page_range                VARCHAR(100),
    ADD COLUMN IF NOT EXISTS early_access_date         VARCHAR(20),
    ADD COLUMN IF NOT EXISTS issn_electronic           VARCHAR(20),
    ADD COLUMN IF NOT EXISTS abstract                  TEXT,
    ADD COLUMN IF NOT EXISTS author_keywords           TEXT,
    ADD COLUMN IF NOT EXISTS wos_categories            TEXT,
    ADD COLUMN IF NOT EXISTS research_areas            TEXT,
    ADD COLUMN IF NOT EXISTS publisher                 VARCHAR(300),
    ADD COLUMN IF NOT EXISTS conference_title          VARCHAR(500),
    ADD COLUMN IF NOT EXISTS times_cited_all_databases INTEGER,
    ADD COLUMN IF NOT EXISTS citing_patents_count      INTEGER,
    ADD COLUMN IF NOT EXISTS funding_orgs              JSONB;

COMMENT ON COLUMN wos_records.accession_number          IS 'Número de acceso interno WoS';
COMMENT ON COLUMN wos_records.wos_categories            IS 'Categorías Web of Science separadas por "; "';
COMMENT ON COLUMN wos_records.research_areas            IS 'Áreas de investigación WoS separadas por "; "';
COMMENT ON COLUMN wos_records.times_cited_all_databases IS 'Citas contadas en todas las colecciones WoS';
COMMENT ON COLUMN wos_records.citing_patents_count      IS 'Número de patentes que citan este artículo';
COMMENT ON COLUMN wos_records.funding_orgs              IS '[{organization, grant_numbers:[]}]';
COMMENT ON COLUMN wos_records.early_access_date         IS 'Fecha de publicación anticipada (Early Access)';
COMMENT ON COLUMN wos_records.issn_electronic           IS 'ISSN de la versión electrónica';


-- ──────────────────────────────────────────────────────────────
-- cvlac_records
-- ──────────────────────────────────────────────────────────────

ALTER TABLE cvlac_records
    ADD COLUMN IF NOT EXISTS isbn           VARCHAR(30),
    ADD COLUMN IF NOT EXISTS product_type  VARCHAR(100),
    ADD COLUMN IF NOT EXISTS abstract       TEXT,
    ADD COLUMN IF NOT EXISTS keywords       TEXT,
    ADD COLUMN IF NOT EXISTS volume         VARCHAR(50),
    ADD COLUMN IF NOT EXISTS issue          VARCHAR(50),
    ADD COLUMN IF NOT EXISTS pages          VARCHAR(100),
    ADD COLUMN IF NOT EXISTS editorial      VARCHAR(300),
    ADD COLUMN IF NOT EXISTS visibility     VARCHAR(50),
    ADD COLUMN IF NOT EXISTS category       VARCHAR(10),
    ADD COLUMN IF NOT EXISTS research_group VARCHAR(300);

COMMENT ON COLUMN cvlac_records.product_type   IS 'Tipo de producto CvLAC: Artículo, Libro, Capítulo, Patente, etc.';
COMMENT ON COLUMN cvlac_records.visibility     IS 'Nacional, Internacional, No Aplica';
COMMENT ON COLUMN cvlac_records.category       IS 'Categoría Minciencias: A1, A2, B, C';
COMMENT ON COLUMN cvlac_records.research_group IS 'Grupo de investigación al que se asocia el producto';


-- ──────────────────────────────────────────────────────────────
-- datos_abiertos_records
-- ──────────────────────────────────────────────────────────────

ALTER TABLE datos_abiertos_records
    ADD COLUMN IF NOT EXISTS isbn           VARCHAR(30),
    ADD COLUMN IF NOT EXISTS product_type  VARCHAR(100),
    ADD COLUMN IF NOT EXISTS volume         VARCHAR(50),
    ADD COLUMN IF NOT EXISTS issue          VARCHAR(50),
    ADD COLUMN IF NOT EXISTS pages          VARCHAR(100),
    ADD COLUMN IF NOT EXISTS editorial      VARCHAR(300),
    ADD COLUMN IF NOT EXISTS country        VARCHAR(100),
    ADD COLUMN IF NOT EXISTS city           VARCHAR(100),
    ADD COLUMN IF NOT EXISTS classification VARCHAR(10),
    ADD COLUMN IF NOT EXISTS visibility     VARCHAR(50),
    ADD COLUMN IF NOT EXISTS research_group VARCHAR(300);

COMMENT ON COLUMN datos_abiertos_records.product_type   IS 'Tipo de producto bibliográfico según Minciencias';
COMMENT ON COLUMN datos_abiertos_records.classification IS 'Clasificación revista/producto: A1, A2, B, C, D';
COMMENT ON COLUMN datos_abiertos_records.visibility     IS 'Nacional, Internacional, No Aplica';
COMMENT ON COLUMN datos_abiertos_records.research_group IS 'Grupo de investigación que reporta el producto';


-- ──────────────────────────────────────────────────────────────
-- Verificación
-- ──────────────────────────────────────────────────────────────

DO $$
BEGIN
    RAISE NOTICE 'Migración v7 completada — expansión de campos en tablas de fuentes.';
    RAISE NOTICE 'openalex_records  : +11 columnas (abstract, keywords, concepts, topics, mesh_terms, oa_url, pdf_url, license, referenced_works_count, apc_paid_usd, grants)';
    RAISE NOTICE 'scopus_records    : +8  columnas (eid, pmid, isbn, index_keywords, subtype_description, conference_name, funding_agency, funding_number)';
    RAISE NOTICE 'wos_records       : +16 columnas (accession_number, pmid, volume, issue, page_range, early_access_date, issn_electronic, abstract, author_keywords, wos_categories, research_areas, publisher, conference_title, times_cited_all_databases, citing_patents_count, funding_orgs)';
    RAISE NOTICE 'cvlac_records     : +11 columnas (isbn, product_type, abstract, keywords, volume, issue, pages, editorial, visibility, category, research_group)';
    RAISE NOTICE 'datos_abiertos_records: +11 columnas (isbn, product_type, volume, issue, pages, editorial, country, city, classification, visibility, research_group)';
END $$;

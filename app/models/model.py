# -- CREATE TABLE llm_retry_queue (
# --     id          SERIAL PRIMARY KEY,
# --     ticket_id   INTEGER NOT NULL,
# --     title       TEXT NOT NULL,
# --     description TEXT NOT NULL,
# --     status      TEXT NOT NULL DEFAULT 'pending',   -- pending | processing | failed
# --     retry_count INTEGER NOT NULL DEFAULT 0,
# --     created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# -- );

# -- CREATE TABLE documents (
# --     id SERIAL PRIMARY KEY,
# --     filename TEXT,
# --     file_path TEXT,
# --     uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# -- );

# -- CREATE TABLE document_chunks (
# --     id SERIAL PRIMARY KEY,
# --     document_id INTEGER,
# --     content TEXT,
# --     embedding VECTOR(768),
# --     page INTEGER,
# --     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# -- );
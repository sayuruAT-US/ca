-- SQL DDL Schema for database initialization / reference
-- All tables share common audit columns and BIGINT primary keys.

-- Ensure schemas exist
CREATE SCHEMA IF NOT EXISTS control_plane;
CREATE SCHEMA IF NOT EXISTS transaction;

-- ============================================================
-- control_plane schema: tenant master data
-- ============================================================

CREATE TABLE IF NOT EXISTS control_plane.tenants (
    id                    BIGINT PRIMARY KEY,
    tenant_id             BIGINT       NOT NULL,
    source_id             BIGINT       NOT NULL,
    bicom_id              BIGINT,
    name                  VARCHAR(255) NOT NULL,
    code                  INTEGER,
    package_id            INTEGER,
    created_by            TEXT         NOT NULL DEFAULT 'system',
    updated_by            TEXT         NOT NULL DEFAULT 'system',
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS control_plane.users (
    id                    BIGSERIAL PRIMARY KEY,
    source_id             BIGINT,
    tenant_id             VARCHAR(255),
    tenant_code           INTEGER,
    bicom_id              BIGINT,
    user_id               VARCHAR(255),
    username              VARCHAR(255),
    password              VARCHAR(255),
    name                  VARCHAR(255),
    email                 VARCHAR(255),
    status                VARCHAR(255),
    role_id               BIGINT       NOT NULL DEFAULT 2,
    failed_attempts       INTEGER      DEFAULT 0,
    is_locked             BOOLEAN      DEFAULT FALSE,
    source_ref_id         TEXT,
    unlock_token          VARCHAR(255),
    unlock_token_expiry   TIMESTAMPTZ,
    created_by            TEXT         NOT NULL DEFAULT 'system',
    updated_by            TEXT         NOT NULL DEFAULT 'system',
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ============================================================
-- transaction schema: operational / sync data
-- ============================================================

CREATE TABLE IF NOT EXISTS transaction.telephony_events (
    id                    BIGSERIAL PRIMARY KEY,
    source_id             BIGINT       NOT NULL,
    module                VARCHAR(255) NOT NULL,
    payload               TEXT         NOT NULL,
    endpoint              VARCHAR(255) NOT NULL,
    status                VARCHAR(255) NOT NULL,
    source_ref_id         TEXT,
    created_by            TEXT         NOT NULL DEFAULT 'system',
    updated_by            TEXT         NOT NULL DEFAULT 'system',
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transaction.departments (
    id                    BIGSERIAL PRIMARY KEY,
    source_id             BIGINT       NOT NULL,
    department_id         BIGINT       NOT NULL,
    tenant_code           INTEGER      NOT NULL,
    tenant_id             BIGINT,
    name                  VARCHAR(255) NOT NULL,
    user_ids              TEXT,
    created_by            TEXT         NOT NULL DEFAULT 'system',
    updated_by            TEXT         NOT NULL DEFAULT 'system',
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uk_departments_tenant_dept UNIQUE (tenant_code, department_id)
);

CREATE TABLE IF NOT EXISTS transaction.extensions (
    id                          BIGSERIAL PRIMARY KEY,
    source_id                   BIGINT       NOT NULL,
    bicom_id                    BIGINT,
    extension_id                BIGINT       NOT NULL,
    tenant_code                 INTEGER      NOT NULL,
    tenant_id                   BIGINT,
    user_id                     VARCHAR(255),
    number                      INTEGER,
    name                        VARCHAR(255),
    email                       VARCHAR(255),
    status                      VARCHAR(50),
    ref                         VARCHAR(255),
    department_ids              TEXT,
    department_admin            VARCHAR(255),
    pin                         VARCHAR(50),
    uad_id                      BIGINT,
    uad_location                VARCHAR(255),
    uad_line_number             VARCHAR(255),
    service_plan_id             BIGINT,
    auto_provisioning_enabled   VARCHAR(50),
    mac_address                 VARCHAR(255),
    serial_number               VARCHAR(255),
    caller_id_pai               VARCHAR(255),
    created_by                  TEXT         NOT NULL DEFAULT 'system',
    updated_by                  TEXT         NOT NULL DEFAULT 'system',
    created_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uk_extensions_tenant_ext UNIQUE (tenant_code, extension_id)
);

CREATE TABLE IF NOT EXISTS transaction.extension_details (
    id                          BIGSERIAL PRIMARY KEY,
    source_id                   BIGINT       NOT NULL,
    bicom_id                    BIGINT,
    extension_id                BIGINT       NOT NULL,
    tenant_code                 INTEGER      NOT NULL,
    tenant_id                   BIGINT,
    number                      INTEGER      NOT NULL,
    user_id                     VARCHAR(255),
    name                        VARCHAR(255),
    email                       VARCHAR(255),
    user_type                   VARCHAR(100),
    dtmf_mode                   VARCHAR(50),
    status                      VARCHAR(50),
    ref                         VARCHAR(255),
    context                     VARCHAR(100),
    title                       VARCHAR(255),
    department_admin            VARCHAR(255),
    moh_id                      INTEGER,
    auth_secret                 VARCHAR(255),
    auth_pin                    VARCHAR(50),
    auth_pbd_pin                VARCHAR(50),
    auth_username               VARCHAR(100),
    uad_id                      BIGINT,
    uad_location                VARCHAR(100),
    uad_label                   VARCHAR(255),
    uad_show_in_directory       VARCHAR(50),
    net_transport               VARCHAR(50),
    net_encryption              VARCHAR(50),
    net_nat                     VARCHAR(50),
    net_direct_media            VARCHAR(50),
    net_qualify                 VARCHAR(50),
    net_host                    VARCHAR(100),
    cid_set_callerid            VARCHAR(50),
    cid_name                    VARCHAR(255),
    cid_number                  VARCHAR(100),
    cid_pai_header_var          VARCHAR(255),
    call_ringtime               INTEGER,
    incoming_limit              INTEGER,
    outgoing_limit              INTEGER,
    voicemail_enabled           VARCHAR(50),
    voicemail_email             VARCHAR(255),
    voicemail_mailbox           INTEGER,
    raw_details                 TEXT,
    created_by                  TEXT         NOT NULL DEFAULT 'system',
    updated_by                  TEXT         NOT NULL DEFAULT 'system',
    created_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uk_extension_details_tenant_num UNIQUE (tenant_code, number)
);

CREATE TABLE IF NOT EXISTS transaction.extension_dids (
    id                          BIGSERIAL PRIMARY KEY,
    source_id                   BIGINT       NOT NULL,
    tenant_code                 INTEGER      NOT NULL,
    tenant_id                   BIGINT,
    extension_id                BIGINT,
    extension_number            INTEGER      NOT NULL,
    did                         VARCHAR(255) NOT NULL,
    raw_response                TEXT,
    created_by                  TEXT         NOT NULL DEFAULT 'system',
    updated_by                  TEXT         NOT NULL DEFAULT 'system',
    created_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uk_extension_dids_tenant_ext_did UNIQUE (tenant_code, extension_number, did)
);

CREATE TABLE IF NOT EXISTS transaction.contacts_details (
    id                          BIGSERIAL PRIMARY KEY,
    source_id                   BIGINT       NOT NULL,
    bicom_id                    VARCHAR(100),
    contact_id                  VARCHAR(100) NOT NULL,
    tenant_code                 INTEGER      NOT NULL,
    tenant_id                   BIGINT,
    source                      VARCHAR(100),
    external_id                 VARCHAR(255),
    scope                       VARCHAR(50),
    first_name                  VARCHAR(255),
    last_name                   VARCHAR(255),
    display_name                VARCHAR(255),
    company                     VARCHAR(255),
    type                        VARCHAR(100),
    primary_phone               VARCHAR(50),
    primary_email               VARCHAR(255),
    phones                      TEXT,
    emails                      TEXT,
    notes                       TEXT,
    fb_user_id                  VARCHAR(255),
    contact_updated_at          BIGINT,
    updated_by_external         VARCHAR(255),
    contact_owner_id            VARCHAR(100),
    contact_owner_name          VARCHAR(255),
    created_by_name             VARCHAR(255),
    permissions                 TEXT,
    raw_details                 TEXT,
    created_by                  TEXT         NOT NULL DEFAULT 'system',
    updated_by                  TEXT         NOT NULL DEFAULT 'system',
    created_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uk_contacts_details_tenant_contact UNIQUE (tenant_code, contact_id)
);

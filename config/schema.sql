PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS nodos (
    id_sala INTEGER PRIMARY KEY,
    nombre TEXT,
    ip TEXT,
    puerto INTEGER,
    estado TEXT DEFAULT 'ACTIVO',
    carga_actual INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pacientes (
    id_paciente INTEGER PRIMARY KEY,
    nombre TEXT,
    seguro_social TEXT UNIQUE,
    fecha_nac TEXT,
    triage INTEGER
);

CREATE TABLE IF NOT EXISTS doctores (
    id_doctor INTEGER PRIMARY KEY,
    nombre TEXT,
    especialidad TEXT,
    carga_actual INTEGER DEFAULT 0,
    capacidad_max INTEGER DEFAULT 3,
    estado TEXT DEFAULT 'DISPONIBLE'
);

CREATE TABLE IF NOT EXISTS camas (
    id_cama INTEGER PRIMARY KEY,
    id_sala INTEGER,
    numero_cama TEXT,
    estado TEXT DEFAULT 'LIBRE',
    FOREIGN KEY(id_sala) REFERENCES nodos(id_sala)
);

CREATE TABLE IF NOT EXISTS visitas (
    id_visita INTEGER PRIMARY KEY AUTOINCREMENT,
    folio TEXT,
    id_paciente INTEGER,
    id_doctor INTEGER,
    id_cama INTEGER,
    id_sala INTEGER,
    fecha_ingreso TEXT DEFAULT CURRENT_TIMESTAMP,
    fecha_salida TEXT,
    estado TEXT DEFAULT 'EN_PROCESO',
    FOREIGN KEY(id_paciente) REFERENCES pacientes(id_paciente),
    FOREIGN KEY(id_doctor) REFERENCES doctores(id_doctor),
    FOREIGN KEY(id_cama) REFERENCES camas(id_cama),
    FOREIGN KEY(id_sala) REFERENCES nodos(id_sala)
);
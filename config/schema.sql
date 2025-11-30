-- Habilitar Foreign Keys en SQLite (Por defecto vienen apagadas)
PRAGMA foreign_keys = ON;

-- Tabla de Nodos (Salas)
CREATE TABLE IF NOT EXISTS nodos (
    id_sala INTEGER PRIMARY KEY,
    nombre VARCHAR(50),
    ip VARCHAR(15),
    puerto INTEGER,
    estado VARCHAR(20) DEFAULT 'ACTIVO',
    carga_actual INTEGER DEFAULT 0
);

-- Tabla Pacientes
CREATE TABLE IF NOT EXISTS pacientes (
    id_paciente INTEGER PRIMARY KEY,
    nombre VARCHAR(100),
    seguro_social VARCHAR(20),
    fecha_nac DATE,
    triage INTEGER
);

-- Tabla Doctores
CREATE TABLE IF NOT EXISTS doctores (
    id_doctor INTEGER PRIMARY KEY,
    nombre VARCHAR(100),
    especialidad VARCHAR(50),
    estado VARCHAR(20) DEFAULT 'DISPONIBLE'
);

-- Tabla Camas
CREATE TABLE IF NOT EXISTS camas (
    id_cama INTEGER PRIMARY KEY,
    id_sala INTEGER,
    numero_cama VARCHAR(10),
    estado VARCHAR(20) DEFAULT 'LIBRE',
    FOREIGN KEY(id_sala) REFERENCES nodos(id_sala)
);

-- Tabla Visitas
CREATE TABLE IF NOT EXISTS visitas (
    id_visita INTEGER PRIMARY KEY AUTOINCREMENT,
    folio VARCHAR(100),
    id_paciente INTEGER,
    id_doctor INTEGER,
    id_cama INTEGER,
    id_sala INTEGER,
    fecha_ingreso DATETIME DEFAULT CURRENT_TIMESTAMP,
    estado VARCHAR(20) DEFAULT 'EN_PROCESO',
    FOREIGN KEY(id_paciente) REFERENCES pacientes(id_paciente),
    FOREIGN KEY(id_doctor) REFERENCES doctores(id_doctor),
    FOREIGN KEY(id_cama) REFERENCES camas(id_cama),
    FOREIGN KEY(id_sala) REFERENCES nodos(id_sala)
);
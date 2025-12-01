from app.data_access.db_manager import DatabaseManager, set_db_context, fetch_all

# Apuntamos al archivo del Nodo 2 específicamente
set_db_context("data/nodo_2.db")

print("--- CONTENIDO DE LA SALA 2 (NODO ESCLAVO) ---")
pacientes = fetch_all("SELECT * FROM pacientes")
for p in pacientes:
    print(f"ID: {p['id_paciente']} | Nombre: {p['nombre']} | Seguro: {p['seguro_social']}")
# Sistema de Viáticos — Sindicato ATE

## Requisitos
- Python 3.9 o superior → https://python.org/downloads
- Conexión a internet solo para la primera instalación

## Instalación y uso

### Windows
1. Descomprimí la carpeta en cualquier lugar (ej: `C:\viaticos-ate\`)
2. Hacé doble clic en **`iniciar.bat`**
3. El navegador se abre automáticamente en `http://localhost:8000`

### Mac / Linux
1. Descomprimí la carpeta
2. Desde la terminal, ejecutá:
   ```bash
   chmod +x iniciar.sh
   ./iniciar.sh
   ```

## Base de datos
El archivo `viaticos.db` se crea automáticamente en la misma carpeta.
**Hacé backup de este archivo regularmente** (copiarlo a Google Drive, pendrive, etc.)

## Funcionalidades
- **Panel de Control**: Vista general del período, estadísticas y tickets pendientes
- **Tickets**: Cargar, revisar (aprobar/rechazar/débito parcial), filtrar y buscar
- **Agentes**: Alta y modificación de agentes con datos bancarios
- **Topes**: Override de topes por agente por mes sin modificar la configuración global
- **Exportar**: Excel (3 hojas) y PDF formal para el período seleccionado

## Acceso desde otras computadoras en la misma red
Una vez iniciado el servidor, otros equipos de la red pueden acceder usando la IP del servidor:
`http://[IP-DEL-SERVIDOR]:8000`

Para ver la IP: ejecutar `ipconfig` (Windows) o `ifconfig` (Mac/Linux)

## Soporte
Sistema desarrollado para el Sindicato ATE.
Base de datos SQLite — sin dependencias externas de servidor.

#!/bin/bash
echo "================================================"
echo "  SISTEMA DE VIÁTICOS ATE"
echo "================================================"
echo ""

# Instalar dependencias si no existen
if ! python3 -c "import fastapi" 2>/dev/null; then
    echo "Instalando dependencias..."
    pip3 install -r requirements.txt
fi

# Obtener IP local
LOCAL_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || ipconfig getifaddr en0 2>/dev/null || echo "ver con: ifconfig")

echo ""
echo "================================================"
echo "  Acceso en ESTA PC:     http://localhost:8000"
echo "  Acceso desde la RED:   http://$LOCAL_IP:8000"
echo "  (compartir esa URL con otras notebooks)"
echo "================================================"
echo ""
echo "Presioná Ctrl+C para detener."
echo ""

# Abrir browser local
(sleep 2 && xdg-open http://localhost:8000 2>/dev/null || open http://localhost:8000 2>/dev/null) &

python3 -m uvicorn app:app --host 0.0.0.0 --port 8000

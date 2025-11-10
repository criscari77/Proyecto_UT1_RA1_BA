from pathlib import Path
DATA = Path(__file__).resolve().parents[1] / "data" / "drops"
DATA.mkdir(parents=True, exist_ok=True)

csv = """fecha,id_cliente,id_producto,unidades,precio_unitario
2025-01-03,C001,P10,2,12.50
2025-01-04,C002,P10,1,12.50
2025-01-04,C001,P20,3,8.00
2025-01-05,C003,P20,1,8.00
2025-01-05,C003,P20,-1,8.00
2025-01-06,C004,P99,2,doce
"""
(DATA / "ventas_ejemplo.csv").write_text(csv, encoding="utf-8")
print("Generado:", DATA / "ventas_ejemplo.csv")

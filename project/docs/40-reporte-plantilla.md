# Reporte Ejecutivo (Caso 4: Encuestas)

> **Titular**: **Satisfacción media de 6.80** con **descenso en Noviembre (5.67)**. Investigar causas del descenso.

## 1) Métricas clave (Encuestas)
- **Total Encuestas Válidas:** 7 (↓ vs 9 filas brutas)
- **Satisfacción General Media:** 6.80 (sobre 10)
- **Tasa de Calidad (Plata / Bronce):** 77.8% (7 de 9 filas válidas)

## 2) Distribución de Satisfacción
(En lugar de "Contribución por producto")

| Puntuación (1-10) | Cantidad | Porcentaje (%) |
| :--- | ---:| ---:|
| 4 | 1 | 14.3 |
| 6 | 1 | 14.3 |
| 7 | 1 | 14.3 |
| 8 | 1 | 14.3 |
| 9 | 1 | 14.3 |
| (5) | (0) | (0.0) |
| (1,2,3,10) | (0) | (0.0) |

## 3) Evolución Mensual
- **Octubre 2024:** 3 encuestas, Satisfacción Media: **8.50**
- **Noviembre 2024:** 4 encuestas, Satisfacción Media: **5.67**
- Se observa un **descenso significativo** en la satisfacción media del último mes.

## 4) Calidad de datos
- Filas procesadas: **bronce 9** · **plata 7** · **quarantine 1**
- Motivos principales de quarantine: **1 fila** por `satisfaccion_general` (valor '11') fuera del rango válido [1-10].

## 5) Próximos pasos
- **Acción 1:** Investigar las causas del descenso de satisfacción en Noviembre (de 8.50 a 5.67).
- **Acción 2:** Contactar con el origen de datos para revisar la fila `1003` enviada a cuarentena.
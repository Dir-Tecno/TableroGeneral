# utils/kpi_tooltips.py
"""
Diccionarios globales de tooltips para KPIs de todo el proyecto.
Incluye tanto descripciones técnicas (categorías de estados) como semánticas (explicaciones amigables).
"""

# Diccionario técnico: lista de estados por KPI
ESTADO_CATEGORIAS = {
    "En Evaluación": ["CREADO","EVALUACIÓN TÉCNICA","COMENZADO"],
    "Rechazados - Bajas": ["RECHAZADO","DESISTIDO","IMPAGO DESISTIDO","BAJA ADMINISTRATIVA"],
    "A Pagar - Convocatoria": ["A PAGAR","A PAGAR CON LOTE","A PAGAR CON BANCO","A PAGAR ENVIADO A SUAF","A PAGAR CON SUAF","MUTUO FIRMADO"],
    "Pagados": ["PAGADO","PRE-FINALIZADO","CON PLAN DE CUOTAS","CON PLAN DE CUOTAS CON IMPAGOS","MOROSO ENTRE 3 Y 4 MESES","MOROSO >= 5 MESES"],
    "Pagados-Finalizados" : ["FINALIZADO"],
    "En proceso de pago": ["PAGO EMITIDO","IMPAGO"]
}

# Diccionario semántico: explicación amigable para cada KPI
TOOLTIPS_DESCRIPTIVOS = {
    "En Evaluación": "Formularios en proceso de evaluación técnica o administrativa (CREADO, EVALUACIÓN TÉCNICA, COMENZADO)",
    "Rechazados - Bajas": "Formularios rechazados o dados de baja por distintos motivos",
    "A Pagar - Convocatoria": "Formularios aprobados listos para pago o en proceso de convocatoria (A PAGAR, A PAGAR CON LOTE, A PAGAR CON BANCO, MUTUO FIRMADO)",
    "Pagados": "Formularios con préstamos ya pagados, finalizados o con plan de cuotas (PAGADO, FINALIZADO, CON PLAN DE CUOTAS, etc.)",
    "En proceso de pago": "Formularios con pago emitido o en estado de impago (PAGO EMITIDO, IMPAGO)",
    "Pagados-Finalizados": "Formularios sin deuda, con préstamos ya pagados (FINALIZADO)"
}

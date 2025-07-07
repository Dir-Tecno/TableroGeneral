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
    "En proceso de pago": ["PAGO EMITIDO","IMPAGO"],
    "Pagados": ["PAGADO","PRE-FINALIZADO","CON PLAN DE CUOTAS","CON PLAN DE CUOTAS CON IMPAGOS","MOROSO ENTRE 3 Y 4 MESES","MOROSO >= 5 MESES"],
    "Pagados-Finalizados" : ["FINALIZADO"],
    "PAGOS GESTIONADOS" : ["IMPAGO DESISTIDO", "FINALIZADO", "PAGADO", "PRE-FINALIZADO", "CON PLAN DE CUOTAS", "CON PLAN DE CUOTAS CON IMPAGOS", "MOROSO ENTRE 3 Y 4 MESES", "MOROSO >= 5 MESES", "PAGO EMITIDO", "IMPAGO"]
}

# Diccionario semántico: explicación amigable para cada KPI
TOOLTIPS_DESCRIPTIVOS = {
    "En Evaluación": "Formularios en proceso de evaluación técnica o administrativa (CREADO, EVALUACIÓN TÉCNICA, COMENZADO)",
    "Rechazados - Bajas": "Formularios rechazados o dados de baja por distintos motivos",
    "A Pagar - Convocatoria": "Formularios aprobados listos para pago o en proceso de convocatoria (A PAGAR, A PAGAR CON LOTE, A PAGAR CON BANCO, MUTUO FIRMADO)",
    "Pagados": "Formularios con préstamos ya pagados, con plan de cuotas (PAGADO, CON PLAN DE CUOTAS, PRE-FINALIZADO,CON PLAN DE CUOTAS CON IMPAGOS,MOROSO ENTRE 3 Y 4 MESES,MOROSO >= 5 MESES)",
    "En proceso de pago": "Formularios con pago emitido o en estado de impago (PAGO EMITIDO, IMPAGO)",
    "Pagados-Finalizados": "Formularios sin deuda, con préstamos ya pagados (FINALIZADO)",
    "PAGOS GESTIONADOS": "Total de formularios con pagos gestionados, incluyendo IMPAGO DESISTIDO y todas las categorías de Pagados-Finalizados, Pagados y En proceso de pago",
    
    # Tooltips para los KPIs de Programas de Empleo
    "BENEFICIARIOS TOTALES": "Total de beneficiarios en programas de empleo, incluyendo Entrenamiento Laboral (EL) y Contrato de Trabajo Indeterminado (CTI)",
    "BENEFICIARIOS EL": "Beneficiarios con categoría de Entrenamiento Laboral",
    "BENEFICIARIOS FIN": "Beneficiarios que completaron el programa de Entrenamiento Laboral",
    "ZONA FAVORECIDA": "Beneficiarios ubicados en departamentos con tratamiento preferencial (zonas con mayor índice de vulnerabilidad)",
    "BENEFICIARIOS CTI": "Beneficiarios con Contrato de Trabajo Indeterminado",
    "EMPRESAS ADHERIDAS": "Total de empresas que se han adherido a los programas de empleo",
    "EMPRESAS CON BENEFICIARIOS": "Empresas que tienen al menos un beneficiario asignado",
    "EMPRESAS SIN BENEFICIARIOS": "Empresas adheridas que no tienen beneficiarios asignados actualmente"
}

# Tooltips para los estados de personas en los programas de empleo
ESTADO_TOOLTIPS = {
    "POSTULANTE APTO": "Las empresas pueden inscribir a los postulantes aptos",
    "INSCRIPTO": "Persona seleccionada por una empresa en el programa de Entrenamiento Laboral, pendiente de análisis",
    "BENEFICIARIO": "Persona beneficiaria del programa de Entrenamiento Laboral actualmente",
    "BENEFICIARIO FIN": "Persona que completaron el programa de Entrenamiento Laboral",
    "INSCRIPTO - CTI": "Persona inscrita en el programa de Contrato de Trabajo Indeterminado",
    "RETENIDO - CTI": "Persona cuyo proceso en CTI está a la espera del alta temprana",
    "VALIDADO - CTI": "Persona que ha sido validada para participar en el programa CTI",
    "BENEFICIARIO- CTI": "Persona con Contrato de Tiempo Indeterminado",
    "BAJA - CTI": "Persona que fue dada de baja del programa CTI",
    "POSTULANTE SIN EMPRESA": "Ficha de persona que se ha postulado pero no tuvo empresa asignada",
    "FUERA CUPO DE EMPRESA": "Ficha de persona que no pudo ser asignada porque la empresa superó su cupo disponible",
    "RECHAZO FORMAL": "Ficha Persona rechazada formalmente del programa por incumplimiento de requisitos",
    "INSCRIPTO NO ACEPTADO": "Persona inscrita pero que no fue seleccionada por ninguna empresa",
    "DUPLICADO": "Registro identificado como duplicado en el sistema",
    "EMPRESA NO APTA": "Ficha de persona no asignada porque la empresa no cumple los requisitos"
}

import uuid

def generate_entity_id():
    """
    Erzeugt eine eindeutige UUID4 für eine Entität.
    Returns:
        str: UUID4 als String
    """
    return str(uuid.uuid4())

def generate_relationship_id():
    """
    Erzeugt eine eindeutige UUID4 für eine Beziehung.
    Returns:
        str: UUID4 als String
    """
    return str(uuid.uuid4())

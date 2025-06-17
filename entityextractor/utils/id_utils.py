import uuid

def generate_entity_id():
    """
    Generates a unique UUID4 for an entity.
    Returns:
        str: UUID4 as string
    """
    return str(uuid.uuid4())

def generate_relationship_id():
    """
    Generates a unique UUID4 for a relationship.
    Returns:
        str: UUID4 as string
    """
    return str(uuid.uuid4())

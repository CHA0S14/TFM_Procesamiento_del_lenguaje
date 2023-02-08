from neomodel import StructuredNode, StringProperty, IntegerProperty, RelationshipTo, RelationshipFrom, config


def connect(url):
    config.DATABASE_URL = url


class FormalConcept(StructuredNode):
    @classmethod
    def category(cls):
        pass

    fcId = IntegerProperty(unique_index=True, required=True)
    intension = RelationshipTo('Attribute', 'INTENSION')
    extension = RelationshipTo('Object', 'EXTENSION')
    parent = RelationshipFrom('FormalConcept', 'PARENT')
    son = RelationshipTo('FormalConcept', 'PARENT')


class Attribute(StructuredNode):
    @classmethod
    def category(cls):
        pass

    name = StringProperty(unique_index=True, required=True)
    formal_concepts = RelationshipFrom('FormalConcept', 'INTENSION')

    def formal_concepts_with_attr(self):
        results, columns = self.cypher("MATCH (a) WHERE id(a)={self} MATCH (a)-[:INTENSION]->(b) RETURN b")
        return [FormalConcept.inflate(row[0]) for row in results]


class Object(StructuredNode):
    @classmethod
    def category(cls):
        pass

    name = StringProperty(unique_index=True, required=True)
    formal_concepts = RelationshipFrom('FormalConcept', 'EXTENSION')


def create_database(db_name, database_connection, verbose=False):
    if verbose:
        print(f'Creating database {db_name}')
    create_db_query = f'CREATE DATABASE {db_name} IF NOT EXISTS'

    database_connection.cypher_query(create_db_query)


def drop_database(db_name, database_connection, verbose=False):
    if verbose:
        print(f'Dropping database {db_name}')
    drop_db_query = f'DROP DATABASE {db_name} IF EXISTS'

    database_connection.cypher_query(drop_db_query)


def get_object_concept(username, database_connection):
    query = '''MATCH (o:Object)<--(f:FormalConcept)
    WHERE o.name = $user
    RETURN f'''

    results, meta = database_connection.cypher_query(query, {"user": username})

    objects = [FormalConcept.inflate(row[0]) for row in results]

    return get_tail(objects), objects


def get_attribute_concept(hotelname, database_connection):
    query = '''MATCH (a:Attribute)<--(f:FormalConcept)
    WHERE a.name = $hotel
    RETURN f'''

    results, meta = database_connection.cypher_query(query, {"hotel": hotelname})

    objects = [FormalConcept.inflate(row[0]) for row in results]

    if len(objects) == 0:
        return None, None

    return get_head(objects), objects


def get_tail(objects):
    fc_ids = [fc.fcId for fc in objects]

    if not fc_ids:
        return None

    return [fc for fc in objects if not fc.son.filter(fcId__in=[id for id in fc_ids if id != fc.fcId])][0]


def get_head(objects):
    fc_ids = [fc.fcId for fc in objects]

    return [fc for fc in objects if not fc.parent.filter(fcId__in=[id for id in fc_ids if id != fc.fcId])][0]

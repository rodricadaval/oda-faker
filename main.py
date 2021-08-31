# python 3.7
# sample use of faker lib / GM@Mongo 20170701
# https://faker.readthedocs.io/en/master/
# https://faker.readthedocs.io/en/master/providers/faker.providers.address.html -

# let's import all libraries (Mongo and Faker)
import sys
import pymongo
from pymongo.errors import BulkWriteError
from faker import Factory
from multiprocessing import Process
import time
import random
import json

# Number of processes to launch
processesNumber = 4
processesList = []

# Settings for Faker, change locale to create other language data
fake = Factory.create('en_US')
# using french names, cities, etc. // fr_FR is almost 10 times faster than en_US

# batch size and bulk size
batchSize = 5000
bulkSize = 100

epocas = ["renacimiento", "moderna", "antigua", "barroco", "greco-Romano", "post-modernismo", "otro"]
estilos = ["moderno", "abstracto", "realismo", "hiperrealismo", "impresionismo", "expresionismo"]
tipo_de_pintura = ["aceite", "agua", "oleo", "otro"]
materiales = ["modera", "piedra", "papel", "lienzo", "otro"]
size = ["alto", "ancho", "volumen", "peso"]
# categorias=["pintura", "escultura", "monumento", "otro"]
obj_categoria = {"pintura": {"tipo_de_pintura": tipo_de_pintura,
                             "material": materiales,
                             "estilo": estilos},
                 "escultura": {"material": materiales,
                               "tamano": size,
                               "estilo": estilos},
                 "monumento": {"material": materiales,
                               "tamano": size,
                               "estilo": estilos},
                 "otro": {"material": materiales}
                 }
# clasificacion=["permanente", "prestado"]
obj_clasificacion = {"permanente": {"fecha": "fake_date",
                                    "estado": ["exhibicion", "prestamo", "almacenada"],
                                    "costo": "fake_money"},
                     "prestado": {"propietario": ["exhibicion", "prestamo", "almacenada"],
                                  "fecha_prestamo": "fake_date",
                                  "fecha_devolucion": "fake_date",
                                  "organismo": "fake_entity",
                                  "seguro": "fake_entity",
                                  "condiciones_de_condiciones": "fake_description"}
                     }

def get_fake_type(data):
    if data.lower() == "fake_date":
        return fake.date()
    elif data.lower() == "fake_entity":
        return fake.company()
    elif data.lower() == "fake_description":
        return fake.text()
    elif data.lower() == "fake_money":
        return fake.currency()
    else:
        return data


def get_fake(data, only_one=0):
    if isinstance(data, list):
        return get_fake(data[random.randint(0, len(data)-1)])
    elif isinstance(data, str):
        return get_fake_type(data)
    elif isinstance(data, dict):
        key = list(data.keys())[random.randint(0, len(data.keys())-1)]
        if only_one == 1:
            return {key: get_fake(data[key], 2)}
        elif only_one == 2:
            return {k: get_fake(v) for k, v in data.items()}
        else:
            return {key: get_fake(data[key])}


def mutate_dict(f,d):
    for k, v in d.iteritems():
        d[k] = f(v)


def generate_random_dict(obj_dict):
    key = obj_dict.keys()[random.randint(0, len(obj_dict.keys()))]
    return mutate_dict(lambda x: get_fake(x), obj_dict[key])


def run(processId):
    # establish a connection to the database
    connection = pymongo.MongoClient("mongodb://localhost")

    # get a handle to the database, and start a bulk op
    db = connection.oda
    obras = db.museo
    bulk = obras.initialize_unordered_bulk_op()

    # let's insert batchSize records
    for i in range(batchSize):
        if (i%bulkSize== 0):
            # print every bulkSize writes
            print('%s - process %s - records %s \n'% (time.strftime("%H:%M:%S"), processId, i))

        if (i%bulkSize == (bulkSize-1)):
            # bulk write
            try:
                bulk.execute()
            except BulkWriteError as bwe:
                pprint(bwe.details)
            bulk = obras.initialize_unordered_bulk_op()
            # and reinit the bulk op

        # Fake person info - this is where you build your people document
        # Create obras record
        try:
            result = bulk.insert({
                                "process": processId,
                                "id": i,
                                "artista": {"nombre": fake.name(),
                                            "nacimiento": fake.date(),
                                            "pais": fake.country(),
                                            "epoca": epocas[random.randint(0, len(epocas)-1)],
                                            "estilo": estilos[random.randint(0, len(estilos)-1)],
                                            "descripcion": fake.text()},
                                "year": random.randint(1500,2020),
                                "title": fake.job(),
                                "descripcion": fake.text(),
                                "historia": [
                                        {"type": "creacion", "number": fake.date()},
                                        {"type": "llegada", "number": fake.text()},
                                        {"type": "perdidas", "number": fake.text()},
                                        {"type": "robos", "number": fake.text()},
                                        {"type": "curiosidades", "number": fake.text()}
                                ],
                                "clasificacion": json.dumps(get_fake(obj_clasificacion, 1)),
                                "pais": fake.country(),
                                "epoca": epocas[random.randint(0, len(epocas)-1)],
                                "categoria": json.dumps(get_fake(obj_categoria, 1)),
                                "exhibido": fake.pybool(),
                                "address": {
                                            "street": fake.street_address(),
                                            "city": fake.city()
                                },
                                "revenue": random.randint(50000, 250000),
                                "age": random.randint(20, 60),
            })
        except Exception as e:
            print("insert failed:{} error : {}".format(i, e))


if __name__ == '__main__':
    # Creation of processesNumber processes
    for i in range(processesNumber):
        process = Process(target=run, args=(i,))
        processesList.append(process)

    # launch processes
    for process in processesList:
        process.start()

    # wait for processes to complete
    for process in processesList:
        process.join()
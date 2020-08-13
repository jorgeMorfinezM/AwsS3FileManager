# -*- coding: utf-8 -*-
"""
Requires Python 3.6 or later
"""

__author__ = "Jorge Morfinez Mojica (jorgemorfinez@ofix.mx)"
__copyright__ = "Copyright 2020, Jorge Morfinez Mojica"
__license__ = "Ofix S.A. de C.V."
__history__ = """ Se conecta, valida y copia 
                  documentos/archivos a un 
                  Bucket de AWS S3 a partir 
                  de un directorio local."""
__version__ = "1.20.H07.1.1.0 ($Rev: 3 $)"


from constants.constants import Constants as Const
import fnmatch
import boto3
from botocore.exceptions import ClientError
from ftplib import FTP_TLS
import argparse
from logger_controller.logger_control import *
import time
import os

logger = configure_logger()


# Conecta a AWS S3 para descargar y leer cada archivo XML
def connect_aws_s3():

    cfg = get_config_constant_file()

    bucket_s3_name = cfg['BUCKET_AWS_S3']['S3_NAME']

    s3_access_key = cfg['BUCKET_AWS_S3']['ACCESS_KEY']
    s3_secret_key = cfg['BUCKET_AWS_S3']['SECRET_KEY']

    bucketname = bucket_s3_name

    s3 = boto3.resource('s3', aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)
    # s3.Bucket(bucketname).upload_file(filename, '/home/ubuntu/environment/ordersS3Uploader/Order-12630.xml')

    bucket_pedidos = s3.Bucket(bucketname)

    # s3.Object(bucketname, pedido).upload_file(Filename=pedido)

    return bucket_pedidos


# Contiene el codigo para conectar Bucket AWS de S3
# y subir el archivo:
def copy_file_to_aws_s3(pedido, file_path):

    cfg = get_config_constant_file()

    bucket_s3_name = cfg['BUCKET_AWS_S3']['S3_NAME']

    s3_access_key = cfg['BUCKET_AWS_S3']['ACCESS_KEY']
    s3_secret_key = cfg['BUCKET_AWS_S3']['SECRET_KEY']

    bucketname = bucket_s3_name

    logger.info('Bucket S3 to Upload file: %s', bucketname)
    logger.info('File to upload: %s', pedido)

    s3 = boto3.resource('s3', aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)

    bucket_pedidos = s3.Bucket(bucketname)

    # s3.Bucket(bucketname).upload_file(pedido, pedido)
    try:
        # response = s3.Object(bucketname, pedido).upload_file(Filename=pedido)
        # response = s3.meta.client.upload_file(file_path, bucketname, pedido)
        response = s3.Object(bucketname, pedido).upload_file(file_path)
    except ClientError as e:
        logging.error(e)
        logging.error(response)
        return False
    return True


# Valida si existe un archivo en el bucket:
def validate_file_exists_s3(pedido_order):

    file_s3_exists = False

    bucket_pedidos = connect_aws_s3()

    logger.info('File to validate in S3: %s', str(pedido_order))

    for pedido in bucket_pedidos.objects.all():
        order_name = pedido.key

        logger.info('File into S3 Bucket: %s', str(order_name))

        if str(pedido_order) in str(order_name):
            file_s3_exists = True
        else:
            file_s3_exists = False

    return file_s3_exists


# Elimina archivo de raiz del bucket S3:
def delete_order_from_s3_root(order_to_delete):
    cfg = get_config_constant_file()

    bucket_pedidos = connect_aws_s3()

    bucket_s3_name = cfg['BUCKET_AWS_S3']['S3_NAME']

    # se usa un pattern para filtrar solo archivos con una extension particular
    pattern = cfg['EXT_ORDERS_TV']

    bucketname = bucket_s3_name

    if fnmatch.fnmatch(order_to_delete, pattern):
        source_s3_order_path = '/' + order_to_delete

        logger.info('Elimina el: %s', 'Archivo: {} de: {}'.format(str(order_to_delete),
                                                                  str(source_s3_order_path)))

        bucket_pedidos.Object(bucketname, order_to_delete).delete()


def parse_xml_files_in_bucket():

    cfg = get_config_constant_file()

    remote_path = cfg['PATH_REMOTE_BUCKET']
    local_temp_path = cfg['PATH_LOCAL']
    pattern = cfg['EXT_ORDERS_TV']

    pedidos = os.listdir(local_temp_path)

    for pedido in pedidos:
        # Validate pattern of all entries that are files

        if fnmatch.fnmatch(pedido, pattern):

            file_remote = remote_path + '/' + pedido
            file_local = local_temp_path + '/' + pedido

            pedido_s3_exists = validate_file_exists_s3(pedido)

            logger.info('File Exists in S3: %s',
                        'File: {0} ¿exists?: {1}'.format(pedido,
                                                         pedido_s3_exists))

            if pedido_s3_exists is False:

                logger.info('Server File >>> ' + file_remote + ' : ' + file_local + ' <<< Local File')

                copy_file_to_aws_s3(pedido, file_local)

                # If file exists, delete it from local path #
                if os.path.isfile(file_local):
                    os.remove(file_local)
                    logger.info('Local File Pedido was deleted: %s', str(file_local))
                else:    # Show an error #
                    logger.error("Error: %s file not found" % file_local)

            else:
                logger.info('File: %s', '{0} already exists in Bucket S3!'.format(pedido))


# Define y obtiene el configurador para las constantes del sistema:
def get_config_constant_file():
    """Contiene la obtencion del objeto config
        para setear datos de constantes en archivo
        configurador

    :rtype: object
    """
    # TEST
    _constants_file = "constants/constants.yml"

    # PROD
    # _constants_file = 'constants/constants.yml'

    cfg = Const.get_constants_file(_constants_file)

    return cfg


def main():
    pass

    parser = argparse.ArgumentParser()

    parser.add_argument('--order_type', required=False, type=str,
                        help="Parametro Tipo de Orden B2C o B2B entre comillas")

    args = parser.parse_args()

    order_type = args.order_type

    # To this system, can contain an argument
    # from executing system and parse to the
    # principal function
    logger.info('ORDER_TYPE ARG: %s', str(order_type))

    parse_xml_files_in_bucket()


if __name__ == "__main__":
    pass

    main()


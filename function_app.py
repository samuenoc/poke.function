import io
import os
import azure.functions as func
import datetime
import json
import logging
import requests
from dotenv import load_dotenv
import pandas as pd
from azure.storage.blob import BlobServiceClient
from typing import Dict, List, Optional
import time

app = func.FunctionApp()
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DOMAIN = os.getenv("DOMAIN")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
STORA_ACCOUNT_NAME = os.getenv("STORA_ACCOUNT_NAME")

@app.queue_trigger(
    arg_name="azqueue",
    queue_name="requests",
    connection="QueueAzureWebJobsStorage"
)
def QueueTriggerPokeReport(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    id = record[0]["id"]
    
    print(f"Procesando solicitud {id} ...")
    update_request(id, "inprogress")
    
    try:
        request_info = get_request(id)
        pokemon_type = request_info[0]["type"]
        
        # Obtener lista básica de Pokémon
        basic_pokemons = get_pokemons(pokemon_type)
        logger.info(f"Obtenidos {len(basic_pokemons)} Pokémon del tipo {pokemon_type}")
        
        # Enriquecer con detalles de cada Pokémon
        detailed_pokemons = get_detailed_pokemons(basic_pokemons)
        logger.info(f"Enriquecidos {len(detailed_pokemons)} Pokémon con detalles")
        
        # Generar CSV con datos enriquecidos
        pokemon_bytes = generate_enhanced_csv_to_blob(detailed_pokemons)
        blob_name = f"poke_report_{id}.csv"
        
        upload_csv_to_blob(blob_name=blob_name, csv_data=pokemon_bytes)
        logger.info(f"Archivo {blob_name} se subió con éxito")
        
        url_completa = f"https://{STORA_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob_name}"
        update_request(id, "completed", url_completa)
        
    except Exception as e:
        logger.error(f"Error procesando solicitud {id}: {str(e)}")
        update_request(id, "failed")
        raise

def update_request(id: int, status: str, url: str = None) -> dict:
    payload = {"status": status, "id": id}
    if url:
        payload["url"] = url
    
    response = requests.put(f"{DOMAIN}/api/request", json=payload)
    return response.json()

def get_request(id: int) -> dict:
    response = requests.get(f"{DOMAIN}/api/request/{id}")
    return response.json()

def get_pokemons(type: str) -> List[Dict]:
    """Obtiene la lista básica de Pokémon de un tipo específico"""
    pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"
    response = requests.get(pokeapi_url, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    pokemon_entries = data.get("pokemon", [])
    return [p["pokemon"] for p in pokemon_entries]

def get_pokemon_details(pokemon_url: str) -> Optional[Dict]:
    """
    Obtiene los detalles completos de un Pokémon individual
    Retorna None si hay error para continuar con los demás
    """
    try:
        response = requests.get(pokemon_url, timeout=20)
        response.raise_for_status()
        pokemon_data = response.json()
        
        # Extraer estadísticas base
        stats = {}
        for stat in pokemon_data.get("stats", []):
            stat_name = stat["stat"]["name"]
            base_stat = stat["base_stat"]
            
            # Mapear nombres más legibles
            stat_mapping = {
                "hp": "HP",
                "attack": "Attack", 
                "defense": "Defense",
                "special-attack": "Special_Attack",
                "special-defense": "Special_Defense",
                "speed": "Speed"
            }
            
            readable_name = stat_mapping.get(stat_name, stat_name.title().replace("-", "_"))
            stats[readable_name] = base_stat
        
        # Extraer habilidades (tomar solo las no ocultas o la primera disponible)
        abilities = []
        hidden_abilities = []
        
        for ability in pokemon_data.get("abilities", []):
            ability_name = ability["ability"]["name"]
            if ability.get("is_hidden", False):
                hidden_abilities.append(ability_name)
            else:
                abilities.append(ability_name)
        
        # Usar habilidades normales, si no hay usar la primera habilidad oculta
        primary_abilities = abilities if abilities else hidden_abilities[:1]
        abilities_str = ", ".join(primary_abilities) if primary_abilities else "Unknown"
        
        # Información adicional útil
        details = {
            "name": pokemon_data.get("name", "Unknown"),
            "id": pokemon_data.get("id", 0),
            "height": pokemon_data.get("height", 0),  # en decímetros
            "weight": pokemon_data.get("weight", 0),   # en hectogramos
            "base_experience": pokemon_data.get("base_experience", 0),
            "abilities": abilities_str,
            **stats  # Agregar todas las estadísticas base
        }
        
        logger.info(f"Detalles obtenidos para {details['name']}")
        return details
        
    except requests.exceptions.RequestException as e:
        logger.warning(f"Error obteniendo detalles de {pokemon_url}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error inesperado procesando {pokemon_url}: {str(e)}")
        return None

def get_detailed_pokemons(basic_pokemons: List[Dict]) -> List[Dict]:
    """
    Enriquece la lista de Pokémon con detalles completos
    """
    detailed_pokemons = []
    total_count = len(basic_pokemons)
    
    for i, pokemon in enumerate(basic_pokemons, 1):
        pokemon_name = pokemon.get("name", "Unknown")
        pokemon_url = pokemon.get("url", "")
        
        logger.info(f"Procesando {i}/{total_count}: {pokemon_name}")
        
        # Obtener detalles del Pokémon
        details = get_pokemon_details(pokemon_url)
        
        if details:
            # Combinar información básica con detalles
            enhanced_pokemon = {
                "original_name": pokemon_name,
                "original_url": pokemon_url,
                **details
            }
            detailed_pokemons.append(enhanced_pokemon)
        else:
            # Si falló, agregar solo la información básica
            logger.warning(f"Usando solo información básica para {pokemon_name}")
            basic_pokemon = {
                "original_name": pokemon_name,
                "original_url": pokemon_url,
                "name": pokemon_name,
                "id": 0,
                "height": 0,
                "weight": 0,
                "base_experience": 0,
                "abilities": "Unknown",
                "HP": 0,
                "Attack": 0,
                "Defense": 0,
                "Special_Attack": 0,
                "Special_Defense": 0,
                "Speed": 0
            }
            detailed_pokemons.append(basic_pokemon)
        
        # Pequeña pausa para evitar sobrecargar la API
        if i < total_count:  # No hacer pausa en el último
            time.sleep(0.1)
    
    logger.info(f"Procesamiento completado. {len(detailed_pokemons)} Pokémon enriquecidos")
    return detailed_pokemons

def generate_enhanced_csv_to_blob(pokemon_list: List[Dict]) -> bytes:
    """
    Genera un CSV enriquecido con estadísticas base y habilidades
    """
    if not pokemon_list:
        logger.warning("Lista de Pokémon vacía")
        # Crear DataFrame vacío con las columnas esperadas
        df = pd.DataFrame(columns=[
            'name', 'id', 'original_url', 'height', 'weight', 
            'base_experience', 'abilities', 'HP', 'Attack', 
            'Defense', 'Special_Attack', 'Special_Defense', 'Speed'
        ])
    else:
        df = pd.DataFrame(pokemon_list)
        
        # Ordenar columnas para mejor legibilidad
        column_order = [
            'name', 'id', 'original_url', 'height', 'weight',
            'base_experience', 'abilities', 'HP', 'Attack', 
            'Defense', 'Special_Attack', 'Special_Defense', 'Speed'
        ]
        
        # Asegurar que todas las columnas existan
        for col in column_order:
            if col not in df.columns:
                df[col] = 0 if col not in ['name', 'abilities', 'original_url'] else 'Unknown'
        
        # Reordenar columnas
        available_columns = [col for col in column_order if col in df.columns]
        df = df[available_columns]
        
        # Agregar algunas estadísticas calculadas
        df['Total_Stats'] = df[['HP', 'Attack', 'Defense', 'Special_Attack', 'Special_Defense', 'Speed']].sum(axis=1)
        df['Height_m'] = df['height'] / 10  # Convertir decímetros a metros
        df['Weight_kg'] = df['weight'] / 10  # Convertir hectogramos a kilogramos
    
    # Generar CSV
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    csv_bytes = output.getvalue().encode('utf-8')
    output.close()
    
    logger.info(f"CSV generado con {len(df)} filas y {len(df.columns)} columnas")
    return csv_bytes

def upload_csv_to_blob(blob_name: str, csv_data: bytes):
    """Sube el archivo CSV al Blob Storage"""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )
        blob_client = blob_service_client.get_blob_client(
            container=BLOB_CONTAINER_NAME, 
            blob=blob_name
        )
        blob_client.upload_blob(csv_data, overwrite=True)
        logger.info(f"Archivo {blob_name} subido exitosamente")
    except Exception as e:
        logger.error(f"Error al subir el archivo {blob_name}: {str(e)}")
        raise
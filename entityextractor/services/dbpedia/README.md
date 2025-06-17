# DBpedia Service

A high-performance, asynchronous service for fetching and processing data from DBpedia. This service is designed to handle batch processing of entities with support for fallback mechanisms and caching.

## Features

- **Batch Processing**: Process multiple entities efficiently using SPARQL batch queries
- **Asynchronous**: Built with `asyncio` for high concurrency
- **Fallback Mechanisms**: Multiple fallback strategies including alternative endpoints and DBpedia Lookup API
- **Caching**: Built-in caching to avoid redundant requests
- **Strict Validation**: Ensures data quality with strict validation rules
- **Configurable**: Customizable timeouts, batch sizes, and endpoints

## Installation

1. Ensure you have Python 3.8+ installed
2. Install the required dependencies:
   ```bash
   pip install aiohttp SPARQLWrapper
   ```

## Usage

### Basic Usage

```python
from entityextractor.services.dbpedia import DBpediaService
from entityextractor.config.settings import load_config

async def main():
    # Load configuration
    config = load_config()
    
    # Initialize the service
    async with DBpediaService(config=config) as service:
        # Process a single entity
        entity = {
            'id': 'e1',
            'name': 'Artificial Intelligence',
            'wikipedia_url': 'https://en.wikipedia.org/wiki/Artificial_intelligence'
        }
        result = await service.process_entity(entity)
        
        # Or process multiple entities
        entities = [
            {'id': 'e1', 'name': 'Berlin', 'wikipedia_url': 'https://en.wikipedia.org/wiki/Berlin'},
            {'id': 'e2', 'name': 'Paris', 'wikipedia_url': 'https://en.wikipedia.org/wiki/Paris'}
        ]
        results = await service.process_entities(entities)

# Run the async function
import asyncio
asyncio.run(main())
```

### Configuration

The service can be configured using a configuration dictionary. Here are the available options:

```python
config = {
    # Request timeout in seconds
    'TIMEOUT_THIRD_PARTY': 30,
    
    # DBpedia SPARQL endpoints (tried in order)
    'DBPEDIA_ENDPOINTS': [
        'https://dbpedia.org/sparql',
        'http://dbpedia.org/sparql',
        'http://live.dbpedia.org/sparql'
    ],
    
    # DBpedia Lookup API endpoint
    'DBPEDIA_LOOKUP_ENDPOINT': 'https://lookup.dbpedia.org/api/search/KeywordSearch',
    
    # Whether to use the Lookup API as a fallback
    'DBPEDIA_USE_LOOKUP': True,
    
    # Batch size for SPARQL queries
    'DBPEDIA_BATCH_SIZE': 50,
    
    # Number of retries for failed requests
    'DBPEDIA_MAX_RETRIES': 3,
    
    # Delay between retries (in seconds)
    'DBPEDIA_RETRY_DELAY': 1.0,
    
    # Default language for results
    'LANGUAGE': 'en',
    
    # Fallback languages (used if data is not available in the primary language)
    'FALLBACK_LANGUAGES': ['en', 'de', 'fr', 'es'],
    
    # User agent for HTTP requests
    'USER_AGENT': 'EntityExtractor/1.0'
}
```

### Response Format

The service returns entities with the following structure:

```python
{
    'id': 'e1',  # Original entity ID
    'status': 'linked',  # 'linked' or 'not_found'
    'uri': 'http://dbpedia.org/resource/Artificial_intelligence',
    'label': 'Artificial intelligence',
    'abstract': 'Artificial intelligence (AI) is the intelligence...',
    'types': ['http://dbpedia.org/ontology/Field', ...],
    'categories': ['Artificial_intelligence', 'Emerging_technologies', ...],
    'part_of': ['http://dbpedia.org/resource/Computer_science', ...],
    'has_part': [...],
    'geo': {
        'lat': '48.8567',
        'long': '2.3508'
    },
    'wiki_url': 'https://en.wikipedia.org/wiki/Artificial_intelligence',
    'homepage': 'https://www.example.com',
    'image_url': 'http://example.com/image.jpg',
    'source': 'sparql',  # or 'lookup' if from the Lookup API
    'raw': { ... }  # Raw response data
}
```

## Error Handling

The service includes comprehensive error handling:

- **Request Timeouts**: Automatically retries failed requests
- **Invalid URIs**: Skips invalid URIs and continues processing
- **Rate Limiting**: Built-in rate limiting to avoid overwhelming the DBpedia servers
- **Fallback Strategies**: Multiple fallback mechanisms for improved reliability

## Examples

See the [examples](../examples/) directory for complete usage examples:

- [Basic Usage](../examples/dbpedia_integration_example.py)
- [Test Script](../examples/test_dbpedia_service.py)

## Development

### Running Tests

```bash
python -m pytest tests/
```

### Code Style

This project follows the [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guide. Use `black` for code formatting:

```bash
pip install black
black .
```

## License

[Your License Here]

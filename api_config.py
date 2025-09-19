import os

# Load Binance API credentials from environment variables

def get_api_credentials():
    """
    Returns Binance API credentials from environment variables.
    Returns:
        tuple: (api_key, api_secret)
    Raises:
        ValueError: If credentials are not set in environment variables.
    """
    api_key = os.getenv('BN_API_KEY')
    api_secret = os.getenv('BN_API_SECRET')
    if api_key is None or api_secret is None:
        raise ValueError('BN_API_KEY and BN_API_SECRET must be set in environment variables.')
    return api_key, api_secret

"""
FIPE API Data Fetcher
=====================
Monolithic script to fetch all car prices from the FIPE table API.
This script recursively fetches brands, models, years, and prices for all vehicles.
"""

import requests
import pandas as pd
import time
import json
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
from typing import Dict, List, Optional, Any
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# API Configuration
FIPE_BASE = "https://veiculos.fipe.org.br"
ENDPOINTS = {
    "tabelas": "/api/veiculos/ConsultarTabelaDeReferencia",
    "marcas": "/api/veiculos/ConsultarMarcas",
    "modelos": "/api/veiculos/ConsultarModelos",
    "modelos_por_ano": "/api/veiculos/ConsultarModelosAtravesDoAno",
    "anos_modelo": "/api/veiculos/ConsultarAnoModelo",
    "valor_todos_params": "/api/veiculos/ConsultarValorComTodosParametros",
}

# Request configuration
HEADERS = {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# Rate limiting
REQUEST_DELAY = 1.0  # Delay between requests in seconds
MAX_RETRIES = 3
RETRY_DELAY = 4

# Vehicle types
VEHICLE_TYPES = {
    1: 'carros',      # Cars
    2: 'motos',       # Motorcycles
    3: 'caminhoes'    # Trucks
}


class FipeAPIClient:
    """Client for interacting with the FIPE API."""
    
    def __init__(self, vehicle_type: int = 1):
        """
        Initialize the FIPE API client.
        
        Args:
            vehicle_type: Type of vehicle (1=cars, 2=motorcycles, 3=trucks)
        """
        self.vehicle_type = vehicle_type
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.base_payload = {"codigoTabelaReferencia": None}
        
    def _make_request(self, endpoint: str, payload: Dict[str, Any], 
                     retries: int = MAX_RETRIES) -> Optional[Any]:
        """
        Make a POST request to the FIPE API with retry logic.
        
        Args:
            endpoint: API endpoint key from ENDPOINTS dict
            payload: Request payload
            retries: Number of retry attempts
            
        Returns:
            JSON response or None if all retries fail
        """
        url = FIPE_BASE + ENDPOINTS[endpoint]
        
        for attempt in range(retries):
            try:
                time.sleep(REQUEST_DELAY)
                response = self.session.post(url, json=payload, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    logger.error(f"All retries failed for endpoint {endpoint}")
                    return None
        return None
    
    def get_reference_table(self) -> Optional[Dict[str, Any]]:
        """
        Get the current reference table (month/year).
        
        Returns:
            Most recent reference table entry
        """
        payload = {}
        result = self._make_request("tabelas", payload)
        if result and len(result) > 0:
            return result[0]  # Return most recent table
        return None
    
    def get_brands(self, table_code: int) -> List[Dict[str, Any]]:
        """
        Get all brands for the specified vehicle type.
        
        Args:
            table_code: Reference table code
            
        Returns:
            List of brands
        """
        payload = {
            "codigoTabelaReferencia": table_code,
            "codigoTipoVeiculo": self.vehicle_type
        }
        result = self._make_request("marcas", payload)
        return result if result else []
    
    def get_models(self, table_code: int, brand_code: str) -> List[Dict[str, Any]]:
        """
        Get all models for a specific brand.
        
        Args:
            table_code: Reference table code
            brand_code: Brand code
            
        Returns:
            List of models
        """
        payload = {
            "codigoTabelaReferencia": table_code,
            "codigoTipoVeiculo": self.vehicle_type,
            "codigoMarca": brand_code
        }
        result = self._make_request("modelos", payload)
        if result and isinstance(result, dict) and 'Modelos' in result:
            return result['Modelos']
        return []
    
    def get_years(self, table_code: int, brand_code: str, 
                 model_code: str) -> List[Dict[str, Any]]:
        """
        Get all available years for a specific model.
        
        Args:
            table_code: Reference table code
            brand_code: Brand code
            model_code: Model code
            
        Returns:
            List of year variants
        """
        payload = {
            "codigoTabelaReferencia": table_code,
            "codigoTipoVeiculo": self.vehicle_type,
            "codigoMarca": brand_code,
            "codigoModelo": model_code
        }
        result = self._make_request("anos_modelo", payload)
        return result if result else []
    
    def get_price(self, table_code: int, brand_code: str, 
                 model_code: str, year_code: str) -> Optional[Dict[str, Any]]:
        """
        Get the price for a specific vehicle configuration.
        
        Args:
            table_code: Reference table code
            brand_code: Brand code
            model_code: Model code
            year_code: Year code (format: "YYYY-F" where F is fuel type)
            
        Returns:
            Price information dict
        """
        # Split year_code into year and fuel type
        year_parts = year_code.split('-')
        year = year_parts[0] if len(year_parts) > 0 else year_code
        fuel_type = year_parts[1] if len(year_parts) > 1 else "1"
        
        payload = {
            "codigoTabelaReferencia": table_code,
            "codigoTipoVeiculo": self.vehicle_type,
            "codigoMarca": brand_code,
            "codigoModelo": model_code,
            "anoModelo": int(year),
            "codigoTipoCombustivel": int(fuel_type),
            "tipoConsulta": "tradicional"
        }
        return self._make_request("valor_todos_params", payload)


class FipeDataFetcher:
    """Main class to orchestrate fetching all FIPE data."""
    
    def __init__(self, vehicle_type: int = 1, output_dir: str = "data/fipe/full"):
        """
        Initialize the data fetcher.
        
        Args:
            vehicle_type: Type of vehicle to fetch (1=cars, 2=motorcycles, 3=trucks)
            output_dir: Directory to save output files
        """
        self.client = FipeAPIClient(vehicle_type)
        self.vehicle_type = vehicle_type
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.data = []
        
    def fetch_all_data(self) -> pd.DataFrame:
        """
        Fetch all data from the FIPE API.
        
        Returns:
            DataFrame with all vehicle prices
        """
        logger.info("Starting FIPE data fetch...")
        
        # Get reference table
        table = self.client.get_reference_table()
        if not table:
            logger.error("Failed to get reference table")
            return pd.DataFrame()
        
        table_code = table['Codigo']
        table_month = table['Mes']
        logger.info(f"Using reference table: {table_month} (Code: {table_code})")
        
        # Get all brands
        brands = self.client.get_brands(table_code)
        logger.info(f"Found {len(brands)} brands")
        
        # Process each brand
        for brand in tqdm(brands, desc="Processing brands"):
            brand_code = brand['Value']
            brand_name = brand['Label']
            
            logger.info(f"Processing brand: {brand_name}")
            
            # Get models for this brand
            models = self.client.get_models(table_code, brand_code)
            
            for model in tqdm(models, desc=f"  {brand_name} models", leave=False):
                model_code = str(model['Value'])
                model_name = model['Label']
                
                # Get years for this model
                years = self.client.get_years(table_code, brand_code, model_code)
                
                for year in years:
                    year_code = year['Value']
                    year_label = year['Label']
                    
                    # Get price for this specific configuration
                    price_data = self.client.get_price(
                        table_code, brand_code, model_code, year_code
                    )
                    
                    if price_data:
                        # Add metadata
                        price_data['BrandCode'] = brand_code
                        price_data['BrandName'] = brand_name
                        price_data['ModelCode'] = model_code
                        price_data['ModelName'] = model_name
                        price_data['YearCode'] = year_code
                        price_data['YearLabel'] = year_label
                        price_data['TableCode'] = table_code
                        price_data['TableMonth'] = table_month
                        price_data['VehicleType'] = self.vehicle_type
                        price_data['VehicleTypeName'] = VEHICLE_TYPES.get(
                            self.vehicle_type, 'unknown'
                        )
                        price_data['FetchDate'] = datetime.now().isoformat()
                        
                        self.data.append(price_data)
            
            # Save progress after each brand
            self._save_progress()
        
        logger.info(f"Fetch complete! Collected {len(self.data)} records")
        return self._create_dataframe()
    
    def _save_progress(self):
        """Save current data to temporary file."""
        if not self.data:
            return
        
        temp_file = self.output_dir / f"fipe_temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
    
    def _create_dataframe(self) -> pd.DataFrame:
        """Convert collected data to DataFrame."""
        if not self.data:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.data)
        
        # Reorder columns for better readability
        column_order = [
            'FetchDate', 'TableMonth', 'TableCode', 'VehicleType', 'VehicleTypeName',
            'BrandName', 'BrandCode', 'ModelName', 'ModelCode', 
            'YearLabel', 'YearCode', 'AnoModelo', 'Combustivel', 'SiglaCombustivel',
            'Valor', 'CodigoFipe', 'MesReferencia', 'DataConsulta', 'Autenticacao',
            'Marca', 'Modelo', 'TipoVeiculo'
        ]
        
        # Add any columns that exist but aren't in our order
        existing_cols = [col for col in column_order if col in df.columns]
        remaining_cols = [col for col in df.columns if col not in existing_cols]
        final_order = existing_cols + remaining_cols
        
        return df[final_order]
    
    def save_to_csv(self, df: pd.DataFrame, filename: Optional[str] = None):
        """
        Save DataFrame to CSV.
        
        Args:
            df: DataFrame to save
            filename: Output filename (default: auto-generated with timestamp)
        """
        if df.empty:
            logger.warning("No data to save")
            return
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d')
            vehicle_type_name = VEHICLE_TYPES.get(self.vehicle_type, 'unknown')
            filename = f"fipe_dump_{timestamp}.csv"
        
        output_path = self.output_dir / filename
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"Data saved to: {output_path}")
        logger.info(f"Total records: {len(df)}")


def main():
    """Main execution function."""
    print("=" * 60)
    print("FIPE Data Fetcher")
    print("=" * 60)
    
    # Configuration
    vehicle_type = 1  # 1=cars, 2=motorcycles, 3=trucks
    
    print(f"\nVehicle type: {VEHICLE_TYPES.get(vehicle_type, 'unknown')}")
    print(f"Output directory: data/fipe/full")
    print("\nStarting data collection...")
    print("This may take several hours depending on the number of vehicles.\n")
    
    # Create fetcher and run
    fetcher = FipeDataFetcher(vehicle_type=vehicle_type)
    
    try:
        df = fetcher.fetch_all_data()
        
        if not df.empty:
            # Save to CSV
            fetcher.save_to_csv(df)
            
            # Print summary statistics
            print("\n" + "=" * 60)
            print("Summary Statistics")
            print("=" * 60)
            print(f"Total records: {len(df)}")
            print(f"Unique brands: {df['BrandName'].nunique()}")
            print(f"Unique models: {df['ModelName'].nunique()}")
            print(f"Year range: {df['AnoModelo'].min()} - {df['AnoModelo'].max()}")
            print(f"Reference month: {df['MesReferencia'].iloc[0]}")
            
            # Top 10 most expensive vehicles
            if 'Valor' in df.columns:
                print("\nTop 10 most expensive vehicles:")
                df_temp = df.copy()
                df_temp['ValorNumerico'] = df_temp['Valor'].str.replace('R$ ', '').str.replace('.', '').str.replace(',', '.').astype(float)
                top_10 = df_temp.nlargest(10, 'ValorNumerico')[['BrandName', 'ModelName', 'AnoModelo', 'Valor']]
                print(top_10.to_string(index=False))
        else:
            print("No data collected. Please check the logs for errors.")
            
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user.")
        print("Saving partial data...")
        if fetcher.data:
            df = fetcher._create_dataframe()
            fetcher.save_to_csv(df, filename=f"fipe_partial_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"\nError occurred: {e}")
        print("Attempting to save partial data...")
        if fetcher.data:
            df = fetcher._create_dataframe()
            fetcher.save_to_csv(df, filename=f"fipe_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")


if __name__ == "__main__":
    main()

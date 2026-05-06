"""Generate realistic financial instrument reference data."""

from __future__ import annotations

import random
import string
from datetime import date, timedelta

import pandas as pd

from data_generation.models.schemas import AssetClass, Currency, Exchange, Sector

# Real-world inspired ticker symbols and company names by sector
_SECTOR_COMPANIES: dict[Sector, list[tuple[str, str]]] = {
    Sector.TECHNOLOGY: [
        ("APEX", "Apex Technologies Inc"),
        ("CYBN", "CyberNet Solutions Corp"),
        ("DGTL", "Digital Frontier Holdings"),
        ("FLUX", "FluxData Systems Inc"),
        ("GRID", "GridPoint Software Corp"),
        ("HXON", "Hexon Cloud Services"),
        ("IONX", "IonX Semiconductor Inc"),
        ("KBYT", "KiloByte Computing Corp"),
        ("LNKD", "LinkedAI Platform Inc"),
        ("MTRX", "Matrix Dynamics Corp"),
        ("NVRA", "Novara Tech Holdings"),
        ("OPTZ", "OptimizeAI Corp"),
        ("PLSR", "Pulsar Digital Inc"),
        ("QBIT", "QuBit Quantum Systems"),
        ("RBOT", "RoboTech Automation Inc"),
        ("SYNX", "SynapseX Networks Corp"),
        ("TERA", "TeraScale Computing"),
        ("VRTX", "VertexAI Solutions Inc"),
        ("WAVR", "WaveRunner Tech Corp"),
        ("ZYNC", "Zync Platforms Inc"),
    ],
    Sector.HEALTHCARE: [
        ("BION", "BioNova Therapeutics"),
        ("CURA", "CuraGen Pharmaceuticals"),
        ("DXMD", "DexMed Diagnostics Inc"),
        ("GENX", "GenXpress Biotech Corp"),
        ("HLTH", "HealthSync Systems Inc"),
        ("IMNO", "ImmunoVax Corp"),
        ("MDVN", "MediVen Life Sciences"),
        ("NURO", "NuroPath Biomedical Inc"),
        ("PHZR", "PhazerBio Inc"),
        ("RXEL", "RxElite Pharma Corp"),
    ],
    Sector.FINANCIALS: [
        ("BNKR", "BankCore Financial Group"),
        ("CPTL", "CapitalEdge Holdings"),
        ("FNDX", "Fundex Asset Management"),
        ("INSR", "InsureNet Corp"),
        ("LDGR", "LedgerPoint Financial"),
        ("MRGN", "MarginCall Capital Inc"),
        ("PRMX", "PrimeX Banking Corp"),
        ("RSKM", "RiskMetrix Financial"),
        ("TRST", "TrustVault Holdings Inc"),
        ("WLTH", "WealthStream Advisors"),
    ],
    Sector.ENERGY: [
        ("CLNR", "CleanR Energy Corp"),
        ("DRLL", "DrillTech Resources Inc"),
        ("ENRG", "EnerGrid Power Holdings"),
        ("FUEL", "FuelCell Dynamics Corp"),
        ("GRDN", "GreenDawn Solar Inc"),
        ("HYDR", "HydroVolt Energy Corp"),
        ("PETR", "PetroMax Exploration Inc"),
        ("SOLR", "SolarEdge Systems Corp"),
        ("VOLT", "VoltPeak Energy Inc"),
        ("WIND", "WindForce Renewables"),
    ],
    Sector.CONSUMER_DISCRETIONARY: [
        ("AUTX", "AutoLux Motors Corp"),
        ("BRND", "BrandHive Retail Inc"),
        ("ECOM", "eComVault Holdings"),
        ("LUXR", "LuxeRetail Group Inc"),
        ("RLTY", "RealtyEdge Properties"),
        ("SHOP", "ShopStream Commerce"),
        ("TRVL", "TravelMax Holdings"),
        ("STRM", "StreamView Media Corp"),
    ],
    Sector.CONSUMER_STAPLES: [
        ("AGRI", "AgriCore Foods Inc"),
        ("BVRG", "BeverageCo Holdings"),
        ("FOOD", "FoodChain Brands Corp"),
        ("GROC", "GrocerNet Distribution"),
        ("NUTR", "NutriVita Health Foods"),
    ],
    Sector.INDUSTRIALS: [
        ("ARSP", "AeroSpace Dynamics Inc"),
        ("CNST", "ConstructionMax Corp"),
        ("DFNS", "DefenseLogic Systems"),
        ("MFGR", "ManufactureX Corp"),
        ("TRNS", "TransPort Logistics Inc"),
        ("STLX", "SteelworX Industries"),
    ],
    Sector.MATERIALS: [
        ("CHEM", "ChemCore Industries Inc"),
        ("GLDM", "GoldMine Resources Corp"),
        ("MTLX", "MetalworX Holdings"),
        ("PLMR", "PolymerTech Materials"),
    ],
    Sector.REAL_ESTATE: [
        ("CMRC", "CommercialRealty Trust"),
        ("HSNG", "HousingVentures REIT"),
        ("PRPT", "PropertyEdge Capital"),
    ],
    Sector.UTILITIES: [
        ("ELEC", "ElectriCo Utilities Inc"),
        ("WATR", "WaterWorks Utility Corp"),
        ("GASX", "GasGrid Distribution"),
    ],
    Sector.COMMUNICATION_SERVICES: [
        ("BRDX", "BroadcastX Media Corp"),
        ("CONN", "ConnectAll Telecom Inc"),
        ("MDIA", "MediaPulse Entertainment"),
        ("STRX", "StreamworX Digital"),
        ("TELX", "TelcoMax Networks Corp"),
    ],
}


def _generate_isin() -> str:
    country = "US"
    nsin = "".join(random.choices(string.ascii_uppercase + string.digits, k=9))
    return f"{country}{nsin}0"


def _generate_cusip() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=9))


class InstrumentGenerator:
    """Generates a universe of financial instruments across asset classes."""

    def __init__(self, num_instruments: int = 500, seed: int = 42):
        self.num_instruments = num_instruments
        self.rng = random.Random(seed)

    def generate(self) -> pd.DataFrame:
        instruments = []
        instrument_id = 1

        # Equities from sector lists
        all_equities = []
        for sector, companies in _SECTOR_COMPANIES.items():
            for symbol, name in companies:
                all_equities.append((symbol, name, sector))

        self.rng.shuffle(all_equities)
        equity_count = min(len(all_equities), int(self.num_instruments * 0.60))

        for symbol, name, sector in all_equities[:equity_count]:
            instruments.append(
                {
                    "instrument_id": f"INS-{instrument_id:06d}",
                    "symbol": symbol,
                    "name": name,
                    "asset_class": AssetClass.EQUITY.value,
                    "exchange": self.rng.choice([Exchange.NYSE, Exchange.NASDAQ, Exchange.BATS, Exchange.IEX]).value,
                    "currency": Currency.USD.value,
                    "sector": sector.value,
                    "isin": _generate_isin(),
                    "cusip": _generate_cusip(),
                    "lot_size": 100,
                    "tick_size": 0.01,
                    "is_active": True,
                    "listed_date": str(
                        date(2020, 1, 1) + timedelta(days=self.rng.randint(0, 1000))
                    ),
                    "expiry_date": None,
                }
            )
            instrument_id += 1

        # ETFs
        etf_tickers = [
            ("SPYX", "S&P 500 Index ETF"),
            ("QQQX", "Nasdaq 100 Index ETF"),
            ("DOWX", "Dow Jones Industrial ETF"),
            ("RUSM", "Russell 2000 Small Cap ETF"),
            ("EMGX", "Emerging Markets ETF"),
            ("BNDX", "Total Bond Market ETF"),
            ("HIYF", "High Yield Corporate Bond ETF"),
            ("GLDX", "Gold Bullion ETF"),
            ("OILX", "Crude Oil Futures ETF"),
            ("RETX", "Real Estate Index ETF"),
            ("TECX", "Technology Sector ETF"),
            ("HLCX", "Healthcare Sector ETF"),
            ("FINX", "Financials Sector ETF"),
            ("DIVX", "High Dividend Yield ETF"),
            ("VOLX", "Volatility Index ETF"),
        ]
        etf_count = min(len(etf_tickers), int(self.num_instruments * 0.12))

        for symbol, name in etf_tickers[:etf_count]:
            instruments.append(
                {
                    "instrument_id": f"INS-{instrument_id:06d}",
                    "symbol": symbol,
                    "name": name,
                    "asset_class": AssetClass.ETF.value,
                    "exchange": self.rng.choice([Exchange.ARCA, Exchange.BATS]).value,
                    "currency": Currency.USD.value,
                    "sector": None,
                    "isin": _generate_isin(),
                    "cusip": _generate_cusip(),
                    "lot_size": 100,
                    "tick_size": 0.01,
                    "is_active": True,
                    "listed_date": str(date(2021, 1, 1) + timedelta(days=self.rng.randint(0, 500))),
                    "expiry_date": None,
                }
            )
            instrument_id += 1

        # Fixed income
        bond_issuers = [
            "US Treasury", "Apple Inc", "Microsoft Corp", "Amazon.com",
            "JPMorgan Chase", "Goldman Sachs", "Bank of America",
            "Verizon Communications", "AT&T Inc", "Johnson & Johnson",
        ]
        fi_count = min(len(bond_issuers) * 3, int(self.num_instruments * 0.15))
        maturities = [2, 5, 7, 10, 20, 30]

        for i in range(fi_count):
            issuer = bond_issuers[i % len(bond_issuers)]
            maturity = self.rng.choice(maturities)
            coupon = round(self.rng.uniform(1.5, 6.5), 3)
            symbol = f"{''.join(w[0] for w in issuer.split()[:2]).upper()}{maturity}Y"
            instruments.append(
                {
                    "instrument_id": f"INS-{instrument_id:06d}",
                    "symbol": symbol,
                    "name": f"{issuer} {coupon}% {maturity}Y Bond",
                    "asset_class": AssetClass.FIXED_INCOME.value,
                    "exchange": Exchange.NYSE.value,
                    "currency": Currency.USD.value,
                    "sector": None,
                    "isin": _generate_isin(),
                    "cusip": _generate_cusip(),
                    "lot_size": 1000,
                    "tick_size": 0.001,
                    "is_active": True,
                    "listed_date": str(date(2022, 1, 1) + timedelta(days=self.rng.randint(0, 365))),
                    "expiry_date": str(date(2024, 1, 1) + timedelta(days=365 * maturity)),
                }
            )
            instrument_id += 1

        # Options (on a subset of equities)
        option_underlyings = [inst for inst in instruments if inst["asset_class"] == AssetClass.EQUITY.value][:20]
        option_count = min(len(option_underlyings) * 4, int(self.num_instruments * 0.10))
        created = 0

        for underlying in option_underlyings:
            if created >= option_count:
                break
            for call_put in ["C", "P"]:
                if created >= option_count:
                    break
                strike_mult = self.rng.uniform(0.85, 1.15)
                base_price = self.rng.uniform(50, 300)
                strike = round(base_price * strike_mult, 0)
                expiry = date(2024, self.rng.choice([3, 6, 9, 12]), self.rng.choice([15, 20]))
                symbol = f"{underlying['symbol']}{expiry.strftime('%y%m%d')}{call_put}{int(strike):05d}"
                instruments.append(
                    {
                        "instrument_id": f"INS-{instrument_id:06d}",
                        "symbol": symbol,
                        "name": f"{underlying['name']} {strike} {call_put} {expiry}",
                        "asset_class": AssetClass.OPTION.value,
                        "exchange": Exchange.CBOE.value,
                        "currency": Currency.USD.value,
                        "sector": underlying.get("sector"),
                        "isin": None,
                        "cusip": None,
                        "lot_size": 100,
                        "tick_size": 0.01,
                        "is_active": True,
                        "listed_date": str(date(2023, 6, 1)),
                        "expiry_date": str(expiry),
                    }
                )
                instrument_id += 1
                created += 1

        # Futures
        futures_specs = [
            ("ES", "E-Mini S&P 500 Futures"),
            ("NQ", "E-Mini Nasdaq 100 Futures"),
            ("YM", "E-Mini Dow Futures"),
            ("CL", "Crude Oil Futures"),
            ("GC", "Gold Futures"),
            ("SI", "Silver Futures"),
            ("ZB", "30-Year T-Bond Futures"),
            ("ZN", "10-Year T-Note Futures"),
            ("6E", "Euro FX Futures"),
            ("6J", "Japanese Yen Futures"),
        ]
        months = ["H", "M", "U", "Z"]  # Mar, Jun, Sep, Dec
        futures_count = min(len(futures_specs) * 2, int(self.num_instruments * 0.05))
        created = 0

        for base_symbol, name in futures_specs:
            if created >= futures_count:
                break
            month = self.rng.choice(months)
            symbol = f"{base_symbol}{month}24"
            instruments.append(
                {
                    "instrument_id": f"INS-{instrument_id:06d}",
                    "symbol": symbol,
                    "name": f"{name} {month}24",
                    "asset_class": AssetClass.FUTURES.value,
                    "exchange": Exchange.CME.value,
                    "currency": Currency.USD.value,
                    "sector": None,
                    "isin": None,
                    "cusip": None,
                    "lot_size": 1,
                    "tick_size": 0.25,
                    "is_active": True,
                    "listed_date": str(date(2023, 9, 1)),
                    "expiry_date": str(date(2024, months.index(month) * 3 + 3, 15)),
                }
            )
            instrument_id += 1
            created += 1

        df = pd.DataFrame(instruments[: self.num_instruments])
        return df

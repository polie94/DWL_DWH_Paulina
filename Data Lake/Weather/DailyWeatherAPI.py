import os
import requests
import json
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the SQLAlchemy engine and sessionmaker
engine = create_engine(os.environ["DB_CONNECTION_STRING"])
Session = sessionmaker(bind=engine)

# Define the base class for the ORM
Base = declarative_base()


# Define the WeatherData class for the ORM
class WeatherData(Base):
    __tablename__ = 'weather_data'

    county = Column(String, primary_key=True)
    date = Column(DateTime, primary_key=True)
    temp_min = Column(Float)
    temp_max = Column(Float)
    app_temp_min = Column(Float)
    app_temp_max = Column(Float)
    precipitation = Column(Float)
    precipitation_hours = Column(Float)
    snowfall = Column(Float)
    sunrise = Column(DateTime)
    sunset = Column(DateTime)


def lambda_handler(event, context):
    # Define the API endpoint URL and parameters
    url = "https://api.open-meteo.com/v1/gfs"
    parameters = {
        "daily": "weathercode,temperature_2m_max,temperature_2m_min,apparent_temperature_max,apparent_temperature_min,"
                 "sunrise,sunset,precipitation_sum,snowfall_sum,"
                 "precipitation_hours",
        "forecast_days": "1",
        "latitude": "",
        "longitude": "",
        "timezone": "auto",
    }

    # Define a list of counties
    counties = [
        {"latitude": 33.97408519, "longitude": -99.77871109, "name": "Foard"},
        {"latitude": 30.26636128, "longitude": -98.39974086, "name": "Blanco"},
        {"latitude": 33.60750375, "longitude": -102.3430919, "name": "Hockley"},
        {"latitude": 30.70566579, "longitude": -98.68387398, "name": "Llano"},
        {"latitude": 34.53064989, "longitude": -101.734951, "name": "Swisher"},
        {"latitude": 33.2160906, "longitude": -94.96509786, "name": "Titus"},
        {"latitude": 32.04695429, "longitude": -96.47248134, "name": "Navarro"},
        {"latitude": 29.19345387, "longitude": -95.45578479, "name": "Brazoria"},
        {"latitude": 26.396627, "longitude": -98.18088681, "name": "Hidalgo"},
        {"latitude": 33.61539176, "longitude": -98.68783437, "name": "Archer"},
        {"latitude": 28.32681413, "longitude": -97.16546961, "name": "Refugio"},
        {"latitude": 32.30506283, "longitude": -102.6378839, "name": "Andrews"},
        {"latitude": 29.39424415, "longitude": -94.96474975, "name": "Galveston"},
        {"latitude": 29.08201599, "longitude": -97.35666976, "name": "De Witt"},
        {"latitude": 29.45641495, "longitude": -97.49279927, "name": "Gonzales"},
        {"latitude": 29.2778362, "longitude": -96.22197375, "name": "Wharton"},
        {"latitude": 33.59396919, "longitude": -96.10656755, "name": "Fannin"},
        {"latitude": 30.96564645, "longitude": -95.92818441, "name": "Madison"},
        {"latitude": 35.83768863, "longitude": -101.8929556, "name": "Moore"},
        {"latitude": 30.7712607, "longitude": -94.37651432, "name": "Tyler"},
        {"latitude": 36.27778432, "longitude": -100.27317, "name": "Lipscomb"},
        {"latitude": 30.49246139, "longitude": -96.62109399, "name": "Burleson"},
        {"latitude": 27.04340499, "longitude": -98.6972921, "name": "Jim Hogg"},
        {"latitude": 33.17753907, "longitude": -99.21236832, "name": "Throckmorton"},
        {"latitude": 27.43274395, "longitude": -97.72889778, "name": "Kleberg"},
        {"latitude": 31.79242191, "longitude": -94.14495261, "name": "Shelby"},
        {"latitude": 33.17991074, "longitude": -101.2984496, "name": "Garza"},
        {"latitude": 28.90493782, "longitude": -97.85960621, "name": "Karnes"},
        {"latitude": 31.32307061, "longitude": -103.693143, "name": "Reeves"},
        {"latitude": 31.31764137, "longitude": -95.42225367, "name": "Houston"},
        {"latitude": 31.7686226, "longitude": -106.2352229, "name": "El Paso"},
        {"latitude": 35.40346802, "longitude": -101.3542037, "name": "Carson"},
        {"latitude": 29.35520493, "longitude": -99.11008331, "name": "Medina"},
        {"latitude": 33.98421585, "longitude": -98.70153389, "name": "Wichita"},
        {"latitude": 29.73912276, "longitude": -94.60891308, "name": "Chambers"},
        {"latitude": 34.96540246, "longitude": -100.8140501, "name": "Donley"},
        {"latitude": 33.14933822, "longitude": -95.56414173, "name": "Hopkins"},
        {"latitude": 31.19627791, "longitude": -98.24142711, "name": "Lampasas"},
        {"latitude": 31.25317017, "longitude": -96.93579511, "name": "Falls"},
        {"latitude": 32.73584155, "longitude": -98.83612016, "name": "Stephens"},
        {"latitude": 30.33423319, "longitude": -97.78194654, "name": "Travis"},
        {"latitude": 35.8385577, "longitude": -100.8135689, "name": "Roberts"},
        {"latitude": 33.61459873, "longitude": -101.2998612, "name": "Crosby"},
        {"latitude": 30.06153035, "longitude": -99.35016969, "name": "Kerr"},
        {"latitude": 32.78634374, "longitude": -95.38206498, "name": "Wood"},
        {"latitude": 33.23345819, "longitude": -98.1725509, "name": "Jack"},
        {"latitude": 32.79797929, "longitude": -94.35725239, "name": "Marion"},
        {"latitude": 34.07051782, "longitude": -101.8269082, "name": "Hale"},
        {"latitude": 30.71559322, "longitude": -104.1403539, "name": "Jeff Davis"},
        {"latitude": 32.77185236, "longitude": -97.29116473, "name": "Tarrant"},
        {"latitude": 30.01093722, "longitude": -95.98768942, "name": "Waller"},
        {"latitude": 31.36617698, "longitude": -101.5230108, "name": "Reagan"},
        {"latitude": 32.37514553, "longitude": -95.26909511, "name": "Smith"},
        {"latitude": 32.56368961, "longitude": -95.8364862, "name": "Van Zandt"},
        {"latitude": 33.638465, "longitude": -97.21247642, "name": "Cooke"},
        {"latitude": 28.86530621, "longitude": -99.76102001, "name": "Zavala"},
        {"latitude": 34.53074134, "longitude": -100.6809876, "name": "Hall"},
        {"latitude": 29.9446397, "longitude": -98.71160511, "name": "Kendall"},
        {"latitude": 28.65708563, "longitude": -97.42646082, "name": "Goliad"},
        {"latitude": 33.17293501, "longitude": -102.8278792, "name": "Yoakum"},
        {"latitude": 33.60614755, "longitude": -99.74144313, "name": "Knox"},
        {"latitude": 31.55234489, "longitude": -97.20184883, "name": "McLennan"},
        {"latitude": 29.81205836, "longitude": -103.2518906, "name": "Brewster"},
        {"latitude": 32.16222886, "longitude": -94.30552858, "name": "Panola"},
        {"latitude": 29.74724753, "longitude": -99.24637282, "name": "Bandera"},
        {"latitude": 34.53014507, "longitude": -102.784594, "name": "Parmer"},
        {"latitude": 32.89786769, "longitude": -96.40780584, "name": "Rockwall"},
        {"latitude": 31.0885069, "longitude": -95.13551682, "name": "Trinity"},
        {"latitude": 31.90050476, "longitude": -97.63437613, "name": "Bosque"},
        {"latitude": 31.88849293, "longitude": -100.5298747, "name": "Coke"},
        {"latitude": 33.17550691, "longitude": -95.21841437, "name": "Franklin"},
        {"latitude": 31.99079033, "longitude": -97.1324142, "name": "Hill"},
        {"latitude": 28.48966896, "longitude": -96.64708119, "name": "Calhoun"},
        {"latitude": 31.30393203, "longitude": -100.9824126, "name": "Irion"},
        {"latitude": 31.86921148, "longitude": -102.0315032, "name": "Midland"},
        {"latitude": 31.77432313, "longitude": -98.99989649, "name": "Brown"},
        {"latitude": 29.17315896, "longitude": -98.08621015, "name": "Wilson"},
        {"latitude": 35.40497586, "longitude": -102.6029949, "name": "Oldham"},
        {"latitude": 31.70486443, "longitude": -96.14918535, "name": "Freestone"},
        {"latitude": 31.34321941, "longitude": -93.85170439, "name": "Sabine"},
        {"latitude": 34.96602014, "longitude": -102.6048162, "name": "Deaf Smith"},
        {"latitude": 31.25475875, "longitude": -94.611742, "name": "Angelina"},
        {"latitude": 33.18284492, "longitude": -100.7791031, "name": "Kent"},
        {"latitude": 33.62074539, "longitude": -95.0501886, "name": "Red River"},
        {"latitude": 29.35000302, "longitude": -100.4178441, "name": "Kinney"},
        {"latitude": 33.205574, "longitude": -97.11681154, "name": "Denton"},
        {"latitude": 30.49747152, "longitude": -100.5382263, "name": "Sutton"},
        {"latitude": 29.58257236, "longitude": -97.94850308, "name": "Guadalupe"},
        {"latitude": 31.85008609, "longitude": -103.0484794, "name": "Winkler"},
        {"latitude": 28.42161168, "longitude": -99.760574, "name": "Dimmit"},
        {"latitude": 33.21585061, "longitude": -97.65444124, "name": "Wise"},
        {"latitude": 35.83750155, "longitude": -100.2704628, "name": "Hemphill"},
        {"latitude": 35.40128927, "longitude": -101.8940484, "name": "Potter"},
        {"latitude": 34.52929257, "longitude": -100.2075664, "name": "Childress"},
        {"latitude": 26.47132899, "longitude": -97.64303323, "name": "Willacy"},
        {"latitude": 30.10353518, "longitude": -97.31187705, "name": "Bastrop"},
        {"latitude": 30.48676251, "longitude": -99.74892716, "name": "Kimble"},
        {"latitude": 32.74720063, "longitude": -100.9165071, "name": "Scurry"},
        {"latitude": 36.27846334, "longitude": -100.815653, "name": "Ochiltree"},
        {"latitude": 36.27753179, "longitude": -101.3545624, "name": "Hansford"},
        {"latitude": 35.83996755, "longitude": -102.6028835, "name": "Hartley"},
        {"latitude": 26.13847728, "longitude": -97.51499968, "name": "Cameron"},
        {"latitude": 28.86711039, "longitude": -95.98533866, "name": "Matagorda"},
        {"latitude": 30.78843382, "longitude": -98.18252382, "name": "Burnet"},
        {"latitude": 32.87004212, "longitude": -95.79347545, "name": "Rains"},
        {"latitude": 30.05782657, "longitude": -98.03104087, "name": "Hays"},
        {"latitude": 30.71742293, "longitude": -99.2263026, "name": "Mason"},
        {"latitude": 31.509424, "longitude": -103.1024636, "name": "Ward"},
        {"latitude": 33.38623601, "longitude": -95.67221685, "name": "Delta"},
        {"latitude": 27.72621549, "longitude": -97.61767326, "name": "Nueces"},
        {"latitude": 32.2977738, "longitude": -99.37359692, "name": "Callahan"},
        {"latitude": 31.03778267, "longitude": -97.47790776, "name": "Bell"},
        {"latitude": 31.83695632, "longitude": -95.16515234, "name": "Cherokee"},
        {"latitude": 32.34839264, "longitude": -96.79467417, "name": "Ellis"},
        {"latitude": 33.6101944, "longitude": -101.8205745, "name": "Lubbock"},
        {"latitude": 30.22502597, "longitude": -102.0764533, "name": "Terrell"},
        {"latitude": 30.72291931, "longitude": -101.4118364, "name": "Crockett"},
        {"latitude": 28.89418432, "longitude": -98.52741302, "name": "Atascosa"},
        {"latitude": 31.86908156, "longitude": -102.5430907, "name": "Ector"},
        {"latitude": 30.6610425, "longitude": -96.3022943, "name": "Brazos"},
        {"latitude": 28.12709562, "longitude": -96.99258951, "name": "Aransas"},
        {"latitude": 32.76653746, "longitude": -96.77781862, "name": "Dallas"},
        {"latitude": 32.30610232, "longitude": -101.4355097, "name": "Howard"},
        {"latitude": 34.07409017, "longitude": -100.7798961, "name": "Motley"},
        {"latitude": 31.39427005, "longitude": -94.16809762, "name": "San Augustine"},
        {"latitude": 28.74260943, "longitude": -100.3144638, "name": "Maverick"},
        {"latitude": 32.23640361, "longitude": -98.21722756, "name": "Erath"},
        {"latitude": 33.61663428, "longitude": -100.7788138, "name": "Dickens"},
        {"latitude": 29.83677935, "longitude": -97.61983479, "name": "Caldwell"},
        {"latitude": 32.74258137, "longitude": -101.9476136, "name": "Dawson"},
        {"latitude": 32.97321771, "longitude": -94.97860185, "name": "Camp"},
        {"latitude": 27.68133382, "longitude": -98.50889672, "name": "Duval"},
        {"latitude": 33.123531, "longitude": -96.08550014, "name": "Hunt"},
        {"latitude": 31.49524259, "longitude": -98.59552736, "name": "Mills"},
        {"latitude": 29.89300587, "longitude": -101.15171, "name": "Val Verde"},
        {"latitude": 30.78100044, "longitude": -102.7235743, "name": "Pecos"},
        {"latitude": 33.61648147, "longitude": -99.21351964, "name": "Baylor"},
        {"latitude": 32.48036452, "longitude": -94.81723694, "name": "Gregg"},
        {"latitude": 33.60410808, "longitude": -102.8285264, "name": "Cochran"},
        {"latitude": 30.74391255, "longitude": -94.02495849, "name": "Jasper"},
        {"latitude": 31.70479181, "longitude": -98.11066654, "name": "Hamilton"},
        {"latitude": 34.53000304, "longitude": -102.2617608, "name": "Castro"},
        {"latitude": 31.82786516, "longitude": -101.0500699, "name": "Sterling"},
        {"latitude": 26.56212627, "longitude": -98.73834895, "name": "Starr"},
        {"latitude": 31.42846287, "longitude": -102.5156425, "name": "Crane"},
        {"latitude": 33.11368507, "longitude": -94.73205137, "name": "Morris"},
        {"latitude": 34.53041048, "longitude": -101.2085769, "name": "Briscoe"},
        {"latitude": 31.4044442, "longitude": -100.4620678, "name": "Tom Green"},
        {"latitude": 30.88981785, "longitude": -99.82061472, "name": "Menard"},
        {"latitude": 32.77852261, "longitude": -97.80472194, "name": "Parker"},
        {"latitude": 30.73899492, "longitude": -95.57224172, "name": "Walker"},
        {"latitude": 27.0315669, "longitude": -98.21874127, "name": "Brooks"},
        {"latitude": 28.34621032, "longitude": -99.1034773, "name": "La Salle"},
        {"latitude": 29.98248216, "longitude": -100.3048937, "name": "Edwards"},
        {"latitude": 30.31065065, "longitude": -96.96568735, "name": "Lee"},
        {"latitude": 31.02705415, "longitude": -96.51272568, "name": "Robertson"},
        {"latitude": 33.17670764, "longitude": -98.68781396, "name": "Young"},
        {"latitude": 28.41736675, "longitude": -97.74116811, "name": "Bee"},
        {"latitude": 33.78379717, "longitude": -98.208958, "name": "Clay"},
        {"latitude": 30.57948905, "longitude": -95.16683273, "name": "San Jacinto"},
        {"latitude": 29.35669829, "longitude": -99.76203107, "name": "Uvalde"},
        {"latitude": 33.17824805, "longitude": -99.73029401, "name": "Haskell"},
        {"latitude": 32.22229511, "longitude": -97.77456553, "name": "Somervell"},
        {"latitude": 29.44884523, "longitude": -98.51966309, "name": "Bexar"},
        {"latitude": 33.62684371, "longitude": -96.67764864, "name": "Grayson"},
        {"latitude": 34.0684915, "longitude": -102.8299069, "name": "Bailey"},
        {"latitude": 32.73970429, "longitude": -99.87850599, "name": "Jones"},
        {"latitude": 29.88425832, "longitude": -94.1708778, "name": "Jefferson"},
        {"latitude": 32.10803224, "longitude": -94.76169577, "name": "Rusk"},
        {"latitude": 31.8694238, "longitude": -101.520742, "name": "Glasscock"},
        {"latitude": 30.31800246, "longitude": -98.94627245, "name": "Gillespie"},
        {"latitude": 32.74305155, "longitude": -100.4019503, "name": "Fisher"},
        {"latitude": 32.21188078, "longitude": -95.85356847, "name": "Henderson"},
        {"latitude": 32.7360276, "longitude": -99.35403034, "name": "Shackelford"},
        {"latitude": 32.75316856, "longitude": -98.31299522, "name": "Palo Pinto"},
        {"latitude": 34.08044953, "longitude": -99.24089568, "name": "Wilbarger"},
        {"latitude": 34.07239775, "longitude": -101.3033037, "name": "Floyd"},
        {"latitude": 30.12807126, "longitude": -93.89653794, "name": "Orange"},
        {"latitude": 31.83105393, "longitude": -99.97624799, "name": "Runnels"},
        {"latitude": 31.77329421, "longitude": -99.45362579, "name": "Coleman"},
        {"latitude": 32.30605847, "longitude": -101.9512464, "name": "Martin"},
        {"latitude": 35.40129002, "longitude": -100.2697634, "name": "Wheeler"},
        {"latitude": 31.44709817, "longitude": -104.5177421, "name": "Culberson"},
        {"latitude": 32.30636024, "longitude": -100.9211128, "name": "Mitchell"},
        {"latitude": 31.45613738, "longitude": -105.3869428, "name": "Hudspeth"},
        {"latitude": 32.7362827, "longitude": -94.94132011, "name": "Upshur"},
        {"latitude": 30.15153187, "longitude": -94.8120925, "name": "Liberty"},
        {"latitude": 30.64834941, "longitude": -97.60105467, "name": "Williamson"},
        {"latitude": 31.61604108, "longitude": -94.6158776, "name": "Nacogdoches"},
        {"latitude": 31.84912973, "longitude": -103.5799063, "name": "Loving"},
        {"latitude": 27.73131099, "longitude": -98.0898649, "name": "Jim Wells"},
        {"latitude": 31.36853957, "longitude": -102.0430005, "name": "Upton"},
        {"latitude": 30.78619977, "longitude": -93.74462993, "name": "Newton"},
        {"latitude": 33.66746186, "longitude": -95.57110784, "name": "Lamar"},
        {"latitude": 30.79250963, "longitude": -94.82994949, "name": "Polk"},
        {"latitude": 33.18789055, "longitude": -96.57248871, "name": "Collin"},
        {"latitude": 34.28865227, "longitude": -99.7453851, "name": "Hardeman"},
        {"latitude": 28.35351091, "longitude": -98.56842256, "name": "McMullen"},
        {"latitude": 34.0776571, "longitude": -100.2786983, "name": "Cottle"},
        {"latitude": 28.86726774, "longitude": -99.10826004, "name": "Frio"},
        {"latitude": 35.40130666, "longitude": -100.812475, "name": "Gray"},
        {"latitude": 36.27777107, "longitude": -102.6021647, "name": "Dallam"},
        {"latitude": 26.92409448, "longitude": -97.68137783, "name": "Kenedy"},
        {"latitude": 29.52763253, "longitude": -95.77090562, "name": "Fort Bend"},
        {"latitude": 34.96522076, "longitude": -101.3572412, "name": "Armstrong"},
        {"latitude": 34.96498493, "longitude": -100.2700733, "name": "Collingsworth"},
        {"latitude": 33.07750446, "longitude": -94.34348771, "name": "Cass"},
        {"latitude": 36.27765824, "longitude": -101.8933822, "name": "Sherman"},
        {"latitude": 33.17373729, "longitude": -102.335157, "name": "Terry"},
        {"latitude": 31.54541898, "longitude": -96.58056718, "name": "Limestone"},
        {"latitude": 29.83182711, "longitude": -99.82215881, "name": "Real"},
        {"latitude": 33.17685487, "longitude": -101.8160235, "name": "Lynn"},
        {"latitude": 29.38437003, "longitude": -96.93020993, "name": "Lavaca"},
        {"latitude": 32.3034718, "longitude": -100.4060989, "name": "Nolan"},
        {"latitude": 32.74074537, "longitude": -102.6351246, "name": "Gaines"},
        {"latitude": 30.30022409, "longitude": -95.50301411, "name": "Montgomery"},
        {"latitude": 31.32645211, "longitude": -99.86413426, "name": "Concho"},
        {"latitude": 30.33238431, "longitude": -94.39021394, "name": "Hardin"},
        {"latitude": 29.85967144, "longitude": -95.39782106, "name": "Harris"},
        {"latitude": 31.29655719, "longitude": -95.99554292, "name": "Leon"},
        {"latitude": 33.44572786, "longitude": -94.42324298, "name": "Bowie"},
        {"latitude": 32.59927808, "longitude": -96.28779341, "name": "Kaufman"},
        {"latitude": 31.81321543, "longitude": -95.65251774, "name": "Anderson"},
        {"latitude": 31.15520543, "longitude": -98.81754295, "name": "San Saba"},
        {"latitude": 30.89661056, "longitude": -100.538097, "name": "Schleicher"},
        {"latitude": 29.6208542, "longitude": -96.52630821, "name": "Colorado"},
        {"latitude": 30.78625299, "longitude": -96.97683495, "name": "Milam"},
        {"latitude": 28.3514375, "longitude": -98.12474493, "name": "Live Oak"},
        {"latitude": 32.7436916, "longitude": -101.4317533, "name": "Borden"},
        {"latitude": 31.19896752, "longitude": -99.34754972, "name": "McCulloch"},
        {"latitude": 32.37899918, "longitude": -97.36660504, "name": "Johnson"},
        {"latitude": 33.67496328, "longitude": -97.72466993, "name": "Montague"},
        {"latitude": 33.17842158, "longitude": -100.255259, "name": "Stonewall"},
        {"latitude": 29.80816362, "longitude": -98.27814516, "name": "Comal"},
        {"latitude": 32.32739258, "longitude": -98.8325713, "name": "Eastland"},
        {"latitude": 31.9490639, "longitude": -98.55841538, "name": "Comanche"},
        {"latitude": 33.61641863, "longitude": -100.2558584, "name": "King"},
        {"latitude": 30.2145299, "longitude": -96.40356575, "name": "Washington"},
        {"latitude": 27.76102524, "longitude": -99.33170164, "name": "Webb"},
        {"latitude": 29.87683207, "longitude": -96.91968047, "name": "Fayette"},
        {"latitude": 32.54814898, "longitude": -94.37155736, "name": "Harrison"},
        {"latitude": 29.99977786, "longitude": -104.2405552, "name": "Presidio"},
        {"latitude": 29.88711223, "longitude": -96.27791829, "name": "Austin"},
        {"latitude": 34.96594288, "longitude": -101.8968527, "name": "Randall"},
        {"latitude": 32.43107878, "longitude": -97.83280338, "name": "Hood"},
        {"latitude": 28.79640594, "longitude": -96.97176602, "name": "Victoria"},
        {"latitude": 35.84000866, "longitude": -101.3547257, "name": "Hutchinson"},
        {"latitude": 31.39087578, "longitude": -97.7991883, "name": "Coryell"},
        {"latitude": 28.95603133, "longitude": -96.57883268, "name": "Jackson"},
        {"latitude": 32.30138229, "longitude": -99.89003941, "name": "Taylor"},
        {"latitude": 27.00089072, "longitude": -99.16860134, "name": "Zapata"},
        {"latitude": 28.00922186, "longitude": -97.51859857, "name": "San Patricio"},
        {"latitude": 30.54360689, "longitude": -95.98557334, "name": "Grimes"},
        {"latitude": 34.06861242, "longitude": -102.3517095, "name": "Lamb"}
    ]

    # Create a list of dictionaries to hold the weather data for each county
    weather_data = []

    # Iterate over the counties and retrieve weather data
    for county in counties:
        # Update the API parameters with the county's coordinates
        parameters["latitude"] = county["latitude"]
        parameters["longitude"] = county["longitude"]

        # Retrieve the weather data from the API
        response = requests.get(url, params=parameters)

        # Parse the JSON response
        data = json.loads(response.text)

        # Extract the daily data and convert it to a Pandas DataFrame
        daily_data = pd.DataFrame(data["daily"])

        # Add the county name to the data dictionary
        daily_data["county"] = county["name"]

        # Rename the "time" column to "date" and reformat the date string
        daily_data = daily_data.rename(columns={"time": "date"})
        daily_data["date"] = pd.to_datetime(daily_data["date"]).dt.strftime("%d-%m-%Y")

        # Set the index to ["county", "date"]
        daily_data = daily_data.set_index(["county", "date"])

        # Add the data dataframe to the list of weather data
        weather_data.append(daily_data)

    # Concatenate the list of dataframes into a single dataframe
    df = pd.concat(weather_data, axis=0, sort=False)
    # Extract only the time part of the "sunrise" and "sunset" columns
    df["sunrise"] = df["sunrise"].str.slice(start=11)
    df["sunset"] = df["sunset"].str.slice(start=11)
    df = df.reset_index()

    # Insert the daily weather data into the database using SQLAlchemy
    session = Session()
    for index, row in df.iterrows():
        weather_data = WeatherData(
            county=row["county"],
            date=datetime.strptime(row["date"], "%d-%m-%Y"),
            temp_min=row["temperature_2m_min"],
            temp_max=row["temperature_2m_max"],
            app_temp_min=row["apparent_temperature_min"],
            app_temp_max=row["apparent_temperature_max"],
            precipitation=row["precipitation_sum"],
            precipitation_hours=row["precipitation_hours"],
            snowfall=row["snowfall_sum"],
            sunrise=datetime.strptime(row["sunrise"], "%H:%M").time(),
            sunset=datetime.strptime(row["sunset"], "%H:%M").time()
        )
        session.merge(weather_data)
    session.commit()
    session.close()

    # Return a success message
    return {
        "statusCode": 200,
        "body": "Weather data retrieved and inserted into the database."
    }

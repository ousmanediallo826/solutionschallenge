import mockData from "./crna_mock_data.json";

// Define the type for an occupation card
type Occupation = {
  occupation: string;
  employment: number;
  hourly_wage: number;
  annual_wage: number;
};

// Cast the imported JSON data to the Occupation[] type
const OccupationCard: Occupation[] = mockData;

export default OccupationCard;



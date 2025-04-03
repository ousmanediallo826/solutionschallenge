import mockData from "./crna_mock_data.json";

// Define the type for an occupation card
type Occupation = {
  occupation: string;
  hourly_wage: number;
  annual_wage: number;
};

// Extract only the required fields from the JSON data
const wages: Occupation[] = mockData.map((item: any) => ({
  occupation: item.occupation,
  hourly_wage: item.hourly_wage,
  annual_wage: item.annual_wage,
}));

export default wages;



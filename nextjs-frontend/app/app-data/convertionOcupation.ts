import mockData from "./crna_mock_data.json";

// Define the type for an occupation card
type Occupation = {
  occupation: string;
  employment: number;
};

// Extract only the required fields from the JSON data
const convertions: Occupation[] = mockData.map((item: any) => ({
  occupation: item.occupation,
  employment: item.employment,
}));

export default convertions;



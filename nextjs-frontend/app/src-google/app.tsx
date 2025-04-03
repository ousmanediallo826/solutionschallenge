import React from "react";
//import PackedBubbleChart from "./circleChart";
import convertions from "../app-data/convertionOcupation";

const transformedData = convertions.map((item) => ({
  name: item.occupation,
  value: item.employment,
}));

export default function App() {
  return (
    <div>
      <h1>Packed Bubble Chart</h1>
      {/*<PackedBubbleChart data={transformedData} />*/}
    </div>
  );
}
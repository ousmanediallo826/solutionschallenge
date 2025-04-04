'use client'

import React from "react";
import { Chart } from "react-google-charts";
import convertions from "../../../../app-data/convertionOcupation"; // Import the data

const getRandomColor = () => {
  const letters = "0123456789ABCDEF";
  let color = "#";
  for (let i = 0; i < 6; i++) {
    color += letters[Math.floor(Math.random() * 16)];
  }
  return color;
};

// Transform the data from convertionOcupation.ts to the format required by Google Charts
const data = [
  ["Occupation", "Employment", { role: "style" }], // Header row
  ...convertions.map((item, index) => [
    item.occupation, // Occupation name
    item.employment, // Employment value
    `color: ${getRandomColor()}`, // Random color for each bar
  ]),
];

export function AppColumn() {
  const options = {
    title: 'Occupational Employment',
    width: '100%',
    height: '100%',
    legend: { position: 'top', maxLines: 3 },
    bar: { groupWidth: '80%' },
    vAxis: {
      title: 'Employment',
      titleTextStyle: { fontSize: 18 },
      textStyle: { fontSize: 14 },
    },
    hAxis: {
      title: 'Occupation',
      titleTextStyle: { fontSize: 18 },
      textStyle: { fontSize: 12 },
      slantedText: true,
      slantedTextAngle: 30,
    },
    titleTextStyle: { fontSize: 24 },
    tooltip: { textStyle: { fontSize: 14 } },
  };

  return (
    <div style={{ width: '900px', height: '700px' }}>
      <Chart
        chartType="ColumnChart"
        width="100%"
        height="100%"
        data={data}
        options={options}
      />
    </div>
  );
}
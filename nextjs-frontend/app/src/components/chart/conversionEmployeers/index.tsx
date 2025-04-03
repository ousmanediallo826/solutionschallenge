import { CirclePercent } from "lucide-react";
import convertions from "../../../../app-data/convertionOcupation";
import { addThousandsSeparator } from "../../utils";
import ChartTitle from "../../titles";
import Chart from "./chartEmployeers";

export default function Convertions() {
  return (
    <section className="flex h-full flex-col gap-2">
      <ChartTitle title="Conversions" icon={CirclePercent} />
      <Indicator />
      <div className="relative max-h-80 flex-grow">
        <Chart />
      </div>
    </section>
  );
}

function Indicator() {
  return (
    <div className="mt-3">
      <span className="mr-1 text-2xl font-medium">
      {addThousandsSeparator(convertions[0]?.employment || 0)}
      </span>
      <span className="text-muted-foreground/60">Employeers </span>
    </div>
  );
}

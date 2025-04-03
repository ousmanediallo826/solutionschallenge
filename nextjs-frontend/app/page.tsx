import Image from "next/image";
import DashboardHome from "./src/dashboard/page";
import {AppColumn} from "./src-google/ColumnChart";
import { WageDashboard } from "./src-google/filterChart";
import wages from "./app-data/wages";
export default function Home() {

  return (
    <>
    <div className="flex items-center gap-2 hover:underline hover:underline-offset-4">
    DASHBOARD
    </div>
    <div>
      All employeers from public, private and government sectors.
    </div>
    <div>
      using google react
    <AppColumn/>
    <WageDashboard wages={wages}/>
    </div>
    <div>
      <p>Using the sample vector</p>
    <DashboardHome/>
    </div>
    </>
  );
}

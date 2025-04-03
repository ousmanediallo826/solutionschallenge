import Image from "next/image";
import DashboardHome from "./src/dashboard/page";
export default function Home() {

  return (
    <>
    <div className="flex items-center gap-2 hover:underline hover:underline-offset-4">
    DASHBOARD
    </div>
    <div>
      All employeers from public, private and government sectors.
    </div>
    <DashboardHome/>
    </>
  );
}

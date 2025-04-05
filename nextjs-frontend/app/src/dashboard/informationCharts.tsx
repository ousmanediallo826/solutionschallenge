export default function Info() {
    return (
        <>
            <div>
                <p className="text-2xl font-bold mb-4 text-gray-800">
                    Correlation Between Charts
                </p>
                <p className="text-lg mb-4 text-gray-600">
                    (Next update there will be new options about correlation)
                </p>
                <ol className="list-decimal pl-6 space-y-4">
                    <li>
                        <span className="font-bold text-blue-400">
                            ColumnChart
                        </span>
                        <br />
                        Occupation job type and amount of employees
                    </li>
                    <li>
                        <span className="font-bold text-green-400">
                            ColumnChart (vertical)
                        </span>
                        <br />
                        Annual wage and hourly wage (selection)
                    </li>
                    <li>
                        <span className="font-bold text-blue-800">
                            Conversion (circle graph)
                        </span>
                        <br />
                        Occupation job type and amount of employees
                    </li>
                </ol>
            </div>
        </>
    );
}
import { Convertions } from "../components/unify";
import Container from "../components/container";
  
export default function Home() {
  return (
    <div>
        <Container className="py-4 laptop:col-span-1">
          <Convertions />
        </Container>
    </div>
  );
}

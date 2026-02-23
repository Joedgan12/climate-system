import { BrowserRouter, Routes, Route, Outlet } from "react-router-dom";
import FontLoader from "./components/FontLoader";
import Nav from "./components/Nav";
import LiveTicker from "./components/LiveTicker";
import Hero from "./components/Hero";
import IngestionPage from "./pages/IngestionPage";
import ComputePage from "./pages/ComputePage";
import StoragePage from "./pages/StoragePage";
import APIPage from "./pages/APIPage";
import ValidationPage from "./pages/ValidationPage";
import GovernancePage from "./pages/GovernancePage";
import Footer from "./components/Footer";

const Layout = () => {
  return (
    <div style={{ minHeight: "100vh", display: "flex", flexDirection: "column" }}>
      <Nav />
      {/* Spacer to handle fixed Nav/Ticker */}
      <div style={{ height: "100px", width: "100%" }} />
      <LiveTicker />
      <main style={{ flex: 1 }}>
        <Outlet />
      </main>
      <Footer />
    </div>
  );
};

export default function App() {
  return (
    <>
      <FontLoader />
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Layout />}>
            <Route index element={<Hero />} />
            <Route path="ingestion" element={<IngestionPage />} />
            <Route path="compute" element={<ComputePage />} />
            <Route path="storage" element={<StoragePage />} />
            <Route path="api-gateway" element={<APIPage />} />
            <Route path="validation" element={<ValidationPage />} />
            <Route path="governance" element={<GovernancePage />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </>
  );
}

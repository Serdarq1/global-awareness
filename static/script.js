 const API_BASE = window.location.origin
 
  async function loadRates(year = 2022) {
    const res = await fetch(`${API_BASE}/rates?year=${year}`)
    if (!res.ok) throw new Error("Failed to load rates")
      return res.json()
  }
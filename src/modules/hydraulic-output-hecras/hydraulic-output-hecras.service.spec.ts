import { Test, TestingModule } from '@nestjs/testing';
import { HydraulicOutputHecrasService } from './hydraulic-output-hecras.service';

describe('HydraulicOutputHecrasService', () => {
  let service: HydraulicOutputHecrasService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [HydraulicOutputHecrasService],
    }).compile();

    service = module.get<HydraulicOutputHecrasService>(HydraulicOutputHecrasService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});

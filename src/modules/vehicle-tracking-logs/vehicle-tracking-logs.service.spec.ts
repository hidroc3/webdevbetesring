import { Test, TestingModule } from '@nestjs/testing';
import { VehicleTrackingLogsService } from './vehicle-tracking-logs.service';

describe('VehicleTrackingLogsService', () => {
  let service: VehicleTrackingLogsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [VehicleTrackingLogsService],
    }).compile();

    service = module.get<VehicleTrackingLogsService>(VehicleTrackingLogsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});

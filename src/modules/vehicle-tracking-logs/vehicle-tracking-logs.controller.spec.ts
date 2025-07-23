import { Test, TestingModule } from '@nestjs/testing';
import { VehicleTrackingLogsController } from './vehicle-tracking-logs.controller';

describe('VehicleTrackingLogsController', () => {
  let controller: VehicleTrackingLogsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [VehicleTrackingLogsController],
    }).compile();

    controller = module.get<VehicleTrackingLogsController>(VehicleTrackingLogsController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
